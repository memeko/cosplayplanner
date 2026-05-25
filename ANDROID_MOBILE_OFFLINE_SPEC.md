# Android Offline Mobile App (Cosplay Planner)

## Что уже готово на backend

Добавлены 2 API-эндпоинта для мобильной синхронизации:

1. `GET /api/mobile/bootstrap`
2. `POST /api/mobile/sync`

Оба работают через текущую авторизацию сессии (cookie), как и остальные API в проекте.

## Сценарий оффлайн-работы

1. При первом входе приложение вызывает `GET /api/mobile/bootstrap` и сохраняет в локальную БД:
   - профиль пользователя;
   - карточки косплана;
   - фестивали.
2. Все изменения пользователь делает локально (Room).
3. Каждое локальное изменение кладётся в очередь синхронизации (`sync_queue`).
4. Когда интернет доступен, `WorkManager` запускает отправку батча в `POST /api/mobile/sync`.

## Контракты API

### 1) Bootstrap

`GET /api/mobile/bootstrap?since=<ISO_DATETIME>`

- если `since` не передан: полный снимок данных;
- если `since` передан: только изменения с указанного времени.

### 2) Push sync

`POST /api/mobile/sync`

Пример тела:

```json
{
  "cards": [
    {
      "client_uid": "local-op-1",
      "id": 123,
      "base_updated_at": "2026-05-22T09:10:11",
      "force": false,
      "resolution": "client_wins",
      "payload": { "character_name": "Asuka", "status_percent": 80 }
    }
  ],
  "festivals": []
}
```

Поддерживаемые `resolution`:

- `client_wins`
- `hybrid`
- `server_wins`

## Конфликты (двое редактируют одну карточку)

Если серверная версия изменилась после `base_updated_at`, API вернет `status: "conflict"` с:

- `conflict_fields`
- `server_record`
- `incoming_payload`
- `suggested_hybrid_payload`

Дальше мобильный клиент должен показать экран сравнения и выбор стратегии:

1. `client_wins` (принудительно перезаписать),
2. `hybrid` (использовать предложенный гибрид),
3. `server_wins` (оставить как на сервере).

После выбора отправка повторяется с `force: true`.

## Архитектура Android (рекомендованная)

- UI: Jetpack Compose
- DI: Hilt
- Local DB: Room
- Background sync: WorkManager
- Network: Retrofit + OkHttp
- Слои:
  - `data/local` (Room entities + DAO)
  - `data/remote` (Retrofit API)
  - `data/repository` (sync orchestration)
  - `domain` (use-cases)
  - `ui` (Compose screens)

## Поддержка планшетов и горизонтальной ориентации

Рекомендуемое поведение:

- телефоны: портрет;
- планшеты (`sw600dp+`): разрешить landscape.

Пример:

`res/values/bools.xml`:

```xml
<resources>
    <bool name="is_tablet">false</bool>
</resources>
```

`res/values-sw600dp/bools.xml`:

```xml
<resources>
    <bool name="is_tablet">true</bool>
</resources>
```

В `MainActivity`:

```kotlin
if (!resources.getBoolean(R.bool.is_tablet)) {
    requestedOrientation = ActivityInfo.SCREEN_ORIENTATION_PORTRAIT
}
```

Это даст горизонтальный режим именно для планшетов.
