# Промпт для Roblox-стороны (Luau / Roblox Studio)

Скопируй всё ниже и отправь нейронке, которая будет делать серверную часть в Roblox Studio.

---

## Задача

У меня есть **Tower Defense** игра в Roblox. Мне нужно добавить серверный модуль, который будет отправлять статистику каждого игрока на внешний сервер (мой Telegram-бот на Python, хостится на Render).

## API-эндпоинты моего сервера

Сервер уже развёрнут. Два endpoint'а:

### 1. `POST /api/match` — отправка результата матча

Вызывать **когда игрок заканчивает раунд** (победа, поражение, выход).

**Headers:**
```
Content-Type: application/json
X-API-Key: <GAME_API_KEY>
```

**Body (JSON):**
```json
{
  "roblox_id": 123456789,
  "roblox_username": "PlayerName",
  "map": "Desert",
  "difficulty": "hard",
  "wave_reached": 25,
  "max_wave": 30,
  "won": false,
  "enemies_killed": 342,
  "bosses_killed": 2,
  "towers_placed": 15,
  "coins_earned": 1500,
  "damage_dealt": 98000,
  "duration_seconds": 620
}
```

**Все поля кроме `roblox_id` и `roblox_username` опциональные (но желательно отправлять всё).**

**Response:** `{"ok": true, "match_id": 42}`

### 2. `POST /api/heartbeat` — пинг активных игроков

Вызывать **каждые 2-5 минут** с серверного скрипта. Нужен чтобы обновлять `last_seen` и знать кто онлайн.

**Headers:** те же.

**Body:**
```json
{
  "players": [
    {"roblox_id": 123456789, "roblox_username": "Player1"},
    {"roblox_id": 987654321, "roblox_username": "Player2"}
  ]
}
```

**Response:** `{"ok": true, "count": 2}`

## Что нужно написать (Luau)

### 1. Модуль `StatsReporter` (ModuleScript в ServerScriptService)

```lua
-- StatsReporter.lua
-- Конфигурация:
local API_URL = "https://<твой-render-домен>" -- заменить
local API_KEY = "<GAME_API_KEY>"              -- заменить на ключ из .env
local HEARTBEAT_INTERVAL = 180                -- секунд (3 мин)
```

Модуль должен:

1. **`StatsReporter:ReportMatch(player, matchData)`**
   - `player` — объект Roblox Player
   - `matchData` — таблица с полями: `map`, `difficulty`, `wave_reached`, `max_wave`, `won`, `enemies_killed`, `bosses_killed`, `towers_placed`, `coins_earned`, `damage_dealt`, `duration_seconds`
   - Отправляет `POST /api/match` через `HttpService:PostAsync()`
   - Должен быть обёрнут в `pcall` чтобы при ошибке сети не крашить сервер
   - Логировать результат: `print("[Stats] Match reported for " .. player.Name)`

2. **Heartbeat-цикл**
   - В `init()` запустить `task.spawn` или корутину
   - Каждые `HEARTBEAT_INTERVAL` секунд собирать всех игроков из `Players:GetPlayers()`
   - Отправлять `POST /api/heartbeat`
   - Тоже обёрнут в `pcall`

### 2. Интеграция в игровой код

В месте где заканчивается раунд (победа/поражение), вызвать:

```lua
local StatsReporter = require(game.ServerScriptService.StatsReporter)

-- Когда раунд закончен:
StatsReporter:ReportMatch(player, {
    map = currentMap.Name,
    difficulty = selectedDifficulty,
    wave_reached = currentWave,
    max_wave = totalWaves,
    won = didWin,
    enemies_killed = playerStats.Kills,
    bosses_killed = playerStats.BossKills,
    towers_placed = playerStats.TowersPlaced,
    coins_earned = playerStats.CoinsEarned,
    damage_dealt = playerStats.DamageDealt,
    duration_seconds = math.floor(roundTimer),
})
```

### 3. При выходе игрока (PlayerRemoving)

Если игрок выходит посреди раунда, тоже отправить данные текущего раунда (если есть активный раунд).

## Важно

- **HttpService** нужно включить в Game Settings → Security → Allow HTTP Requests
- API_KEY должен совпадать с `GAME_API_KEY` в `.env` файле бота
- Если сервер не отвечает — не крашить игру, просто `warn()` в логи
- НЕ отправлять данные из клиента (LocalScript) — только из серверных скриптов (ServerScript / ServerScriptService), иначе эксплойтеры смогут подделать статы
- `HttpService:PostAsync()` может бросить ошибку при недоступности сервера — ВСЕГДА оборачивать в `pcall`
- При нескольких игроках в одном раунде — вызывать `ReportMatch` для КАЖДОГО игрока отдельно

## Пример готового модуля-скелета

```lua
-- ServerScriptService/StatsReporter.lua
local HttpService = game:GetService("HttpService")
local Players = game:GetService("Players")

local StatsReporter = {}
StatsReporter.__index = StatsReporter

local API_URL = "https://YOUR-APP.onrender.com"  -- ЗАМЕНИТЬ
local API_KEY = "change-me-secret-key-123"        -- ЗАМЕНИТЬ
local HEARTBEAT_INTERVAL = 180

local function postJSON(endpoint, data)
    local url = API_URL .. endpoint
    local body = HttpService:JSONEncode(data)
    local headers = {
        ["Content-Type"] = "application/json",
        ["X-API-Key"] = API_KEY,
    }
    local ok, result = pcall(function()
        return HttpService:PostAsync(url, body, Enum.HttpContentType.ApplicationJson, false, headers)
    end)
    if ok then
        local decoded = HttpService:JSONDecode(result)
        return true, decoded
    else
        warn("[StatsReporter] HTTP error: " .. tostring(result))
        return false, result
    end
end

function StatsReporter:ReportMatch(player, matchData)
    local data = {
        roblox_id = player.UserId,
        roblox_username = player.Name,
        map = matchData.map,
        difficulty = matchData.difficulty,
        wave_reached = matchData.wave_reached or 0,
        max_wave = matchData.max_wave,
        won = matchData.won or false,
        enemies_killed = matchData.enemies_killed or 0,
        bosses_killed = matchData.bosses_killed or 0,
        towers_placed = matchData.towers_placed or 0,
        coins_earned = matchData.coins_earned or 0,
        damage_dealt = matchData.damage_dealt or 0,
        duration_seconds = matchData.duration_seconds or 0,
    }
    
    task.spawn(function()
        local ok, res = postJSON("/api/match", data)
        if ok then
            print("[Stats] Match reported for " .. player.Name .. " (id=" .. tostring(res.match_id) .. ")")
        end
    end)
end

-- Heartbeat loop
task.spawn(function()
    while true do
        task.wait(HEARTBEAT_INTERVAL)
        local players = Players:GetPlayers()
        if #players > 0 then
            local list = {}
            for _, p in ipairs(players) do
                table.insert(list, {
                    roblox_id = p.UserId,
                    roblox_username = p.Name,
                })
            end
            postJSON("/api/heartbeat", { players = list })
        end
    end
end)

return StatsReporter
```

## URL сервера

Заменить `YOUR-APP.onrender.com` на актуальный домен с Render. Ключ `GAME_API_KEY` — задай в `.env` файле бота и в Roblox-скрипте одинаковый.

## Переменные окружения (.env бота)

Добавить в `.env`:
```
GAME_API_KEY=тут-придумай-длинный-секретный-ключ
```

---

**Готово.** После добавления этого модуля в Roblox Studio, статистика начнёт приходить в Telegram-бот. Игроки смогут смотреть свои стати через `/stats`, привязывать аккаунт через `/link`, и видеть лидерборд через `/top`.
