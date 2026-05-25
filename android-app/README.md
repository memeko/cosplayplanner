# Cosplay Planner Android App (MVP Skeleton)

Этот модуль добавляет базовый каркас Android-приложения с offline-first подходом:

- локальное хранение: `Room`
- API: `Retrofit`
- фоновая синхронизация: `WorkManager`
- DI: `Hilt`
- UI: `Jetpack Compose`

## Что уже реализовано

1. Структура проекта Android (`android-app/`).
2. Модели API для новых backend-эндпоинтов:
   - `GET /api/mobile/bootstrap`
   - `POST /api/mobile/sync`
3. Локальные сущности:
   - профиль пользователя
   - карточки
   - фестивали
   - очередь синхронизации
4. Планшеты:
   - на телефонах фиксируем портрет
   - на `sw600dp+` разрешаем landscape

## Важно

`MobileSyncWorker` сейчас добавлен как безопасная заглушка. Следующий шаг: подключить в него `SyncRepository.pushPendingChanges()` через Hilt Worker.
