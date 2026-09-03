#!/usr/bin/env python3
"""
Модуль синхронизации папки NewsRAG с Google Drive через Google Drive API.

Поддерживает:
1. Google Service Account (service_account.json) — рекомендовано для серверов/облака 24/7.
2. OAuth 2.0 User Flow (credentials.json + token.json) — для персонального использования.
3. Офлайн / Dry-Run режим — если ключи еще не настроены, сообщает инструкцию без краша пайплайна.
4. Обновление агрегированных файлов NOTEBOOKLM/*.md (update) и загрузку новых RAW файлов (create).
5. Локальный кэш ID папок и файлов (gdrive_cache.json) для быстродействия.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

# =================== КОНФИГУРАЦИЯ ===================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOCAL_DIR = os.path.join(SCRIPT_DIR, 'NewsRAG')
DEFAULT_SERVICE_ACCOUNT = os.path.join(SCRIPT_DIR, 'service_account.json')
DEFAULT_OAUTH_CREDS = os.path.join(SCRIPT_DIR, 'credentials.json')
DEFAULT_TOKEN_FILE = os.path.join(SCRIPT_DIR, 'token.json')
CACHE_FILE = os.path.join(SCRIPT_DIR, 'gdrive_cache.json')
LOG_FILE = os.path.join(SCRIPT_DIR, 'gdrive_sync.log')

ROOT_FOLDER_NAME = "NewsRAG"
SCOPES = ['https://www.googleapis.com/auth/drive']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)


# =================== КЭШ СОСТОЯНИЯ ===================

def load_cache() -> dict:
    """Загружает локальный кэш ID папок и файлов на Google Drive."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log.warning("⚠️ Не удалось прочитать кэш Google Drive: %s", e)
    return {"folders": {}, "files": {}}


def save_cache(cache: dict):
    """Сохраняет локальный кэш ID папок и файлов на Google Drive."""
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.warning("⚠️ Не удалось сохранить кэш Google Drive: %s", e)


# =================== АВТОРИЗАЦИЯ ===================

def get_drive_service(service_account_path: str | None = None,
                      oauth_creds_path: str | None = None):
    """
    Авторизуется в Google Drive API.
    Возвращает ресурс service или None, если ключи не найдены.
    """
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
    except ImportError:
        log.error("❌ Библиотека google-api-python-client не установлена. Запустите: pip install -r requirements.txt")
        return None

    # 1. Проверяем Service Account (приоритет для облака/сервера)
    sa_path = service_account_path or os.environ.get('GDRIVE_SERVICE_ACCOUNT') or DEFAULT_SERVICE_ACCOUNT
    if os.path.exists(sa_path):
        log.info("🔑 Используем Google Service Account: %s", sa_path)
        try:
            creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            log.error("❌ Ошибка авторизации через Service Account: %s", e)
            return None

    # 2. Проверяем OAuth User Flow
    oa_creds_path = oauth_creds_path or os.environ.get('GDRIVE_OAUTH_CREDS') or DEFAULT_OAUTH_CREDS
    token_path = os.environ.get('GDRIVE_TOKEN_PATH') or DEFAULT_TOKEN_FILE

    creds = None
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception as e:
            log.warning("⚠️ Недействительный токен %s: %s", token_path, e)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, 'w', encoding='utf-8') as token_f:
                    token_f.write(creds.to_json())
            except Exception as e:
                log.warning("⚠️ Не удалось обновить токен: %s", e)
                creds = None

        if not creds and os.path.exists(oa_creds_path):
            log.info("🔑 Авторизация через OAuth 2.0 (credentials.json)...")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(oa_creds_path, SCOPES)
                creds = flow.run_local_server(port=0)
                with open(token_path, 'w', encoding='utf-8') as token_f:
                    token_f.write(creds.to_json())
            except Exception as e:
                log.error("❌ Ошибка OAuth авторизации: %s", e)
                return None

    # 3. Проверяем Application Default Credentials (ADC от gcloud)
    try:
        import google.auth
        adc_creds, _ = google.auth.default(scopes=SCOPES)
        if adc_creds:
            log.info("🔑 Используем Google Application Default Credentials (ADC)...")
            return build('drive', 'v3', credentials=adc_creds)
    except Exception:
        pass

    if creds and creds.valid:
        return build('drive', 'v3', credentials=creds)

    log.info("ℹ️ Google Drive API ключи не настроены.")
    log.info("   Для подключения синхронизации:")
    log.info("   1. Либо выполните в терминале: gcloud auth application-default login --scopes='https://www.googleapis.com/auth/drive'")
    log.info("   2. Либо поместите 'service_account.json' / 'credentials.json' в корень проекта: %s", SCRIPT_DIR)
    return None


# =================== ОПЕРАЦИИ С GOOGLE DRIVE ===================

def get_or_create_drive_folder(service, folder_name: str, parent_id: str | None = None,
                               cache: dict | None = None) -> str:
    """Находит или создает папку на Google Drive и возвращает её folder_id."""
    cache_key = f"{parent_id or 'root'}/{folder_name}"
    if cache and cache_key in cache.get('folders', {}):
        return cache['folders'][cache_key]

    folder_id = None

    # Если это корневая папка NewsRAG (parent_id is None):
    safe_folder = folder_name.replace("'", "\\'")
    if not parent_id:
        # 1. Сначала ищем папку, которой пользователь поделился с сервисным аккаунтом
        query_shared = f"mimeType='application/vnd.google-apps.folder' and name='{safe_folder}' and trashed=false and sharedWithMe=true"
        res = service.files().list(
            q=query_shared,
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        items = res.get('files', [])

        if items:
            folder_id = items[0]['id']
            log.info("📂 Найдена папка '%s' на вашем Google Диске (ID: %s)", folder_name, folder_id)

    # 2. Если не найдена в sharedWithMe, ищем обычным запросом
    if not folder_id:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{safe_folder}' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        items = results.get('files', [])
        if items:
            folder_id = items[0]['id']

    # 3. Если папка не найдена — создаем
    if not folder_id:
        metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            metadata['parents'] = [parent_id]
        
        folder = service.files().create(
            body=metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        folder_id = folder.get('id')
        log.info("📁 Создана папка на Google Drive: '%s' (ID: %s)", folder_name, folder_id)

    if cache is not None:
        cache.setdefault('folders', {})[cache_key] = folder_id

    return folder_id


def sync_file_to_drive(service, local_filepath: str, drive_folder_id: str,
                       cache: dict, dry_run: bool = False) -> tuple[str, bool]:
    """
    Загружает или обновляет файл на Google Drive.
    Возвращает: (action: 'uploaded' | 'updated' | 'skipped', success: bool)
    """
    from googleapiclient.http import MediaFileUpload

    filename = os.path.basename(local_filepath)
    file_mtime = os.path.getmtime(local_filepath)
    file_size = os.path.getsize(local_filepath)
    cache_key = f"{drive_folder_id}/{filename}"

    file_info = cache.get('files', {}).get(cache_key)

    # Если файл уже загружен и размер/время не менялись — пропускаем
    if file_info and file_info.get('mtime') == file_mtime and file_info.get('size') == file_size:
        return 'skipped', True

    if dry_run:
        log.info("🔍 [DRY-RUN] Загрузка файла %s -> папка %s", filename, drive_folder_id)
        return 'uploaded', True

    media = MediaFileUpload(local_filepath, mimetype='text/markdown', resumable=True)

    try:
        # Если файл уже был в кэше, пробуем обновить
        if file_info and file_info.get('id'):
            file_id = file_info['id']
            updated_file = service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id, name',
                supportsAllDrives=True
            ).execute()
            
            cache.setdefault('files', {})[cache_key] = {
                'id': updated_file.get('id'),
                'mtime': file_mtime,
                'size': file_size
            }
            return 'updated', True

        # Ищем файл в папке на Drive (на случай если кэш был сброшен)
        safe_filename = filename.replace("'", "\\'")
        query = f"name='{safe_filename}' and '{drive_folder_id}' in parents and trashed=false"
        res = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        existing = res.get('files', [])

        if existing:
            file_id = existing[0]['id']
            updated_file = service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id, name',
                supportsAllDrives=True
            ).execute()
            action = 'updated'
            saved_id = updated_file.get('id')
        else:
            file_metadata = {
                'name': filename,
                'parents': [drive_folder_id]
            }
            created_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name',
                supportsAllDrives=True
            ).execute()
            action = 'uploaded'
            saved_id = created_file.get('id')

        cache.setdefault('files', {})[cache_key] = {
            'id': saved_id,
            'mtime': file_mtime,
            'size': file_size
        }
        return action, True

    except Exception as e:
        err_msg = str(e)
        if 'storageQuotaExceeded' in err_msg:
            log.error("❌ Google Drive квота: Сервисный аккаунт не может создавать файлы вне расшаренной папки.")
            log.error("👉 Создайте папку 'NewsRAG' на вашем Google Диске и нажмите 'Поделиться' на email сервисного аккаунта.")
        else:
            log.error("❌ Ошибка загрузки файла %s на Drive: %s", filename, e)
        return 'error', False


# =================== ОСНОВНАЯ СИНХРОНИЗАЦИЯ ===================

def sync_to_gdrive(local_dir: str = DEFAULT_LOCAL_DIR,
                   root_folder_name: str = ROOT_FOLDER_NAME,
                   service_account_path: str | None = None,
                   oauth_creds_path: str | None = None,
                   dry_run: bool = False) -> dict:
    """
    Синхронизирует локальную директорию NewsRAG с Google Drive.
    
    Возвращает статистику:
    {'uploaded': int, 'updated': int, 'skipped': int, 'errors': int, 'synced': bool}
    """
    stats = {'uploaded': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'synced': False}

    if not os.path.exists(local_dir):
        log.warning("⚠️ Локальная директория не существует: %s", local_dir)
        return stats

    service = get_drive_service(
        service_account_path=service_account_path,
        oauth_creds_path=oauth_creds_path
    )

    if not service and not dry_run:
        log.info("⏸️ Синхронизация с Google Drive пропущена (нет авторизации). Файлы сохранены локально в %s.", local_dir)
        return stats

    cache = load_cache()

    log.info("☁️ Начинаем синхронизацию %s -> Google Drive ('%s')...", local_dir, root_folder_name)

    # 1. Получаем/создаем корневую папку NewsRAG
    if dry_run and not service:
        root_folder_id = "mock_root_id"
    else:
        root_folder_id = get_or_create_drive_folder(service, root_folder_name, cache=cache)

    # Словарь сопоставления локального пути к ID папки Drive
    dir_to_drive_id = {str(Path(local_dir).resolve()): root_folder_id}

    # 2. Обходим все файлы и подпапки
    for root, dirs, files in os.walk(local_dir):
        current_path = str(Path(root).resolve())
        current_drive_id = dir_to_drive_id.get(current_path)

        if not current_drive_id:
            continue

        # Создаем подпапки на Google Drive
        for d in dirs:
            subdir_path = str(Path(os.path.join(root, d)).resolve())
            if dry_run and not service:
                sub_drive_id = f"mock_{d}_id"
            else:
                sub_drive_id = get_or_create_drive_folder(
                    service=service,
                    folder_name=d,
                    parent_id=current_drive_id,
                    cache=cache
                )
            dir_to_drive_id[subdir_path] = sub_drive_id

        # Синхронизируем файлы
        for f in files:
            if not f.endswith('.md'):
                continue
            local_filepath = os.path.join(root, f)
            action, success = sync_file_to_drive(
                service=service,
                local_filepath=local_filepath,
                drive_folder_id=current_drive_id,
                cache=cache,
                dry_run=dry_run
            )

            if success:
                if action == 'uploaded':
                    stats['uploaded'] += 1
                elif action == 'updated':
                    stats['updated'] += 1
                elif action == 'skipped':
                    stats['skipped'] += 1
            else:
                stats['errors'] += 1

    save_cache(cache)
    stats['synced'] = True

    log.info("✅ Синхронизация Google Drive завершена: Загружено=%d, Обновлено=%d, Пропущено=%d, Ошибок=%d",
             stats['uploaded'], stats['updated'], stats['skipped'], stats['errors'])

    return stats


# =================== CLI ===================

def main():
    parser = argparse.ArgumentParser(description="Синхронизация Markdown новостей с Google Drive")
    parser.add_argument('--dir', default=DEFAULT_LOCAL_DIR, help="Локальная директория с новостями (NewsRAG)")
    parser.add_argument('--folder-name', default=ROOT_FOLDER_NAME, help="Имя корневой папки на Google Drive")
    parser.add_argument('--service-account', default=None, help="Путь к service_account.json")
    parser.add_argument('--credentials', default=None, help="Путь к credentials.json")
    parser.add_argument('--dry-run', action='store_true', help="Имитация синхронизации без реальных сетевых запросов")
    args = parser.parse_args()

    stats = sync_to_gdrive(
        local_dir=args.dir,
        root_folder_name=args.folder_name,
        service_account_path=args.service_account,
        oauth_creds_path=args.credentials,
        dry_run=args.dry_run
    )

    print("\n" + "=" * 50)
    print("📊 ИТОГ СИНХРОНИЗАЦИИ GOOGLE DRIVE:")
    print(f"   Новых файлов загружено: {stats['uploaded']}")
    print(f"   Файлов обновлено:       {stats['updated']}")
    print(f"   Пропущено (без изм.):   {stats['skipped']}")
    print(f"   Ошибок:                 {stats['errors']}")
    print(f"   Статус:                 {'Успешно' if stats['synced'] else 'Офлайн (локально)'}")
    print("=" * 50)


if __name__ == '__main__':
    main()
