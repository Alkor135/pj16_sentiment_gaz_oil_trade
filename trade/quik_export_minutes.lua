--[[
    quik_export_minutes.lua

    Экспортирует минутные свечи M1 из QUIK в CSV, который подсасывает
    rts/download_minutes_to_db.py для добивки последних 15 минут сессии,
    недоступных через ISS API из-за 15-минутной задержки.

    Запуск: "Сервисы → Lua скрипты → Добавить" в QUIK, затем "Запустить".
    Скрипт висит весь день, но реально пишет файл только в окне 20:00–21:05 МСК.

    Формат CSV: SECID,TRADEDATE,OPEN,LOW,HIGH,CLOSE,VOLUME
                (колонки идентичны схеме таблицы Futures в sqlite-БД минут)

    Файл перезаписывается каждые PERIOD_MS миллисекунд в активном окне и
    всегда содержит ровно последние TAIL_BARS баров по каждому тикеру
    (т.е. не разрастается).
]]

local OUT       = "C:\\Users\\Alkor\\VSCode\\pj16_sentiment_gaz_oil_trade\\trade\\quik_export\\minutes.csv"
local OUT_TMP   = OUT .. ".tmp"
local CLASS     = "SPBFUT"
local TICKERS   = {"RIM6", "RIU6", "MXM6", "MXU6"}
local PERIOD_MS = 5000
local TAIL_BARS = 60

local is_run = true
local ds_map = {}


function OnInit()
    for _, t in ipairs(TICKERS) do
        local ds, err = CreateDataSource(CLASS, t, INTERVAL_M1)
        if ds then
            ds:SetEmptyCallback()
            ds_map[t] = ds
        else
            message("quik_export_minutes: не создался DS " .. t .. ": " .. tostring(err), 3)
        end
    end
end


function OnStop()
    is_run = false
    return 5
end


local function fmt_ts(t)
    return string.format("%04d-%02d-%02d %02d:%02d:00",
        t.year, t.month, t.day, t.hour, t.min)
end


local function in_active_window()
    local t = os.date("*t")
    if t.hour == 9 then return true end
    if t.hour == 21 and t.min <= 5 then return true end
    return false
end


local function dump()
    local f, err = io.open(OUT_TMP, "w")
    if not f then
        message("quik_export_minutes: io.open failed: " .. tostring(err), 3)
        return
    end

    f:write("SECID,TRADEDATE,OPEN,LOW,HIGH,CLOSE,VOLUME\n")

    for tkr, ds in pairs(ds_map) do
        local n = ds:Size()
        if n > 0 then
            local start_i = math.max(1, n - TAIL_BARS + 1)
            for i = start_i, n do
                f:write(string.format("%s,%s,%s,%s,%s,%s,%s\n",
                    tkr,
                    fmt_ts(ds:T(i)),
                    tostring(ds:O(i)),
                    tostring(ds:L(i)),
                    tostring(ds:H(i)),
                    tostring(ds:C(i)),
                    tostring(ds:V(i))
                ))
            end
        end
    end

    f:close()

    -- Атомарная подмена, чтобы Python не прочитал наполовину записанный файл
    os.remove(OUT)
    os.rename(OUT_TMP, OUT)
end


function main()
    while is_run do
        if in_active_window() then
            pcall(dump)
            sleep(PERIOD_MS)
        else
            sleep(60000)
        end
    end
end
