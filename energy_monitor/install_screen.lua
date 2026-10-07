local ROLE = "screen"
local DEFAULT_DIR = "/root/energy_monitor/screen"
local EXECUTABLE = {"run_screen.sh"}
local KEEP_IF_EXISTS = { ["config.lua"] = true }

local files = {
    { name = "main.lua", content = [=[
local scriptDir = (arg and arg[0] or ""):match("^(.*)/[^/]*$") or "."
package.path = scriptDir .. "/?.lua;" .. package.path

local socket = require("socket")
local config = require("config")
local utils = require("utils")
local protocol = require("protocol")
local Client = require("network")
local Renderer = require("renderer")

local ESC = "\27["

local USAGE = [[
Usage: lua main.lua [options]
  --demo                 affiche des donnees de demonstration (sans reseau)
  --data-dir <dossier>   dossier des logs (defaut: <programme>/data)
  --central <ip>         adresse IP du PC central (defaut: config.lua)
]]

local function parseArguments(args)
    local options = {}
    local index = 1
    while args[index] do
        local name = args[index]
        if name == "--demo" then
            options.demo = true
            index = index + 1
        elseif name == "--data-dir" then
            options.dataDir = args[index + 1]
            index = index + 2
        elseif name == "--central" then
            options.central = args[index + 1]
            index = index + 2
        else
            return nil
        end
    end
    if options.dataDir == "" or options.central == "" then
        return nil
    end
    return options
end

local function installSignalHandlers()
    local ok, signal = pcall(require, "posix.signal")
    if not ok then
        return
    end
    local function handler()
        utils.stopRequested = true
    end
    pcall(signal.signal, signal.SIGTERM, handler)
    pcall(signal.signal, signal.SIGINT, handler)
end

local function probeSize()
    local handle = io.popen("stty size 2>/dev/null")
    if not handle then
        return nil
    end
    local output = handle:read("l")
    handle:close()
    if not output then
        return nil
    end
    local rows, columns = output:match("^(%d+)%s+(%d+)$")
    rows, columns = tonumber(rows), tonumber(columns)
    if rows and columns and rows > 0 and columns > 0 then
        return rows, columns
    end
    return nil
end

local function demoSnapshot(step)
    local magnitudes = { 950, 15400, 2.8e6, 4.2e9, 1.1e12 }
    local feSecond = magnitudes[step % #magnitudes + 1]
    return {
        type = "snapshot",
        seq = step,
        status = "online",
        bus = "ok",
        production = { j_t = feSecond / 8, fe_t = feSecond / 20, fe_s = feSecond },
        generators = { total = 30, active = 24, invalid = 2, stale = 0 },
        storage = { fe = feSecond * 3, capacity_fe = feSecond * 10 },
        cumulative = { fe = feSecond * 86400 },
        stats = { avg_1m_fe_s = feSecond * 0.98, avg_1h_fe_s = feSecond * 0.95, peak_fe_s = feSecond * 1.1 },
        sources = {
            wind = { enabled = true, total = 30, active = 24, fe_t = feSecond / 20, fe_s = feSecond },
            solar = { enabled = false },
            nuclear = { enabled = false },
        },
    }
end

local function run(options)
    utils.setConversion(config.energy.feNumerator, config.energy.feDenominator)
    if options.central then
        config.centralAddress = options.central
    end

    local dataDir = options.dataDir or (scriptDir .. "/data")
    utils.mkdirP(dataDir)
    local log = utils.newLogger({
        path = dataDir .. "/" .. config.log.fileName,
        maxBytes = config.log.maxBytes,
        repeatInterval = config.log.repeatInterval,
        echo = false,
    })
    log:info("Ecran demarre" .. (options.demo and " (demo)" or ""))

    local client = nil
    if not options.demo then
        local created, err = Client.new(config, log)
        if not created then
            io.stderr:write("Reseau indisponible: " .. tostring(err) .. "\n")
            log:error("Erreur reseau: socket impossible (" .. tostring(err) .. ")")
            log:close()
            return 1
        end
        client = created
    end

    local state = { snapshot = nil, lastReceive = nil, online = false, protocolError = false }
    local columns, rows = config.fallbackColumns, config.fallbackRows
    local nextProbe, nextSubscribe, nextRender = 0, 0, 0
    local lastColumns, lastRows = nil, nil
    local wasOnline = nil
    local demoStep, nextDemo = 0, 0

    io.write(ESC .. "2J" .. ESC .. "?25l")
    io.flush()

    while not utils.shouldStop() do
        local now = utils.now()

        if options.demo then
            if now >= nextDemo then
                demoStep = demoStep + 1
                state.snapshot = demoSnapshot(demoStep)
                state.lastReceive = now
                nextDemo = now + 3
            end
        else
            if now >= nextSubscribe then
                client:subscribe()
                nextSubscribe = now + (state.online and config.subscribeInterval or config.retryInterval)
            end
            local snapshot = client:receive()
            if snapshot then
                state.snapshot = snapshot
                state.lastReceive = now
            end
            state.protocolError = client.lastError == "unsupported_version"
        end

        state.online = state.lastReceive ~= nil and (now - state.lastReceive) <= config.offlineAfter
        if state.online ~= wasOnline then
            if state.online then
                log:info("CENTRAL ONLINE")
            elseif wasOnline ~= nil or state.lastReceive ~= nil then
                log:warn("CENTRAL OFFLINE")
            end
            wasOnline = state.online
        end

        if now >= nextRender then
            if now >= nextProbe then
                local probedRows, probedColumns = probeSize()
                rows = probedRows or config.fallbackRows
                columns = probedColumns or config.fallbackColumns
                nextProbe = now + config.sizeProbeInterval
            end
            if columns ~= lastColumns or rows ~= lastRows then
                io.write(ESC .. "2J")
                lastColumns, lastRows = columns, rows
            end
            local ok, result, width = pcall(Renderer.build, state, now, columns, rows, config.maxWidth)
            if ok then
                local text = Renderer.toString(result, width, columns, config.useColor)
                io.write(ESC .. "H" .. text .. "\n" .. ESC .. "J")
                io.flush()
            else
                log:event("render", "ERROR", "Erreur affichage: " .. tostring(result))
            end
            nextRender = now + config.renderInterval
        end

        local wait = math.min(nextRender, options.demo and nextRender or nextSubscribe) - utils.now()
        if wait < 0.01 then
            wait = 0.01
        end
        if client then
            socket.select({ client:socket() }, nil, wait)
        else
            socket.sleep(wait)
        end
    end

    io.write(ESC .. "?25h" .. ESC .. "0m\n")
    io.flush()
    if client then
        client:unsubscribe()
        client:close()
    end
    log:info("Ecran arrete")
    log:close()
    return 0
end

local options = parseArguments(arg or {})
if not options then
    io.stderr:write(USAGE)
    os.exit(2)
end
installSignalHandlers()
local status = run(options)
os.exit(status)
]=] },
    { name = "config.lua", content = [[
return {
    centralAddress = "10.0.0.1",
    centralPort = 4710,
    clientId = "screen-1",

    subscribeInterval = 5.0,
    retryInterval = 2.0,
    offlineAfter = 5.0,
    renderInterval = 0.5,
    sizeProbeInterval = 5.0,

    fallbackColumns = 80,
    fallbackRows = 30,
    maxWidth = 72,
    useColor = true,

    energy = {
        feNumerator = 2,
        feDenominator = 5,
    },

    log = {
        fileName = "screen.log",
        maxBytes = 32768,
        repeatInterval = 60,
    },
}
]] },
    { name = "utils.lua", content = [[
local socket = require("socket")

local utils = {}

local haveTime, ptime = pcall(require, "posix.time")

utils.stopRequested = false

local UNITS = {
    { 1, "FE" },
    { 1e3, "kFE" },
    { 1e6, "MFE" },
    { 1e9, "GFE" },
    { 1e12, "TFE" },
}

local feNumerator, feDenominator = 2, 5

function utils.now()
    if haveTime then
        local ts = ptime.clock_gettime(ptime.CLOCK_MONOTONIC)
        return ts.tv_sec + ts.tv_nsec / 1e9
    end
    return socket.gettime()
end

function utils.shouldStop()
    return utils.stopRequested
end

function utils.setConversion(numerator, denominator)
    feNumerator, feDenominator = numerator, denominator
end

function utils.joulesToFe(joules)
    joules = tonumber(joules) or 0
    if joules <= 0 then
        return 0
    end
    return math.floor(joules * feNumerator / feDenominator + 0.5)
end

function utils.joulesToFeExact(joules)
    return joules * feNumerator / feDenominator
end

function utils.round(value)
    return math.floor(value + 0.5)
end

function utils.formatEnergy(value, suffix)
    value = tonumber(value)
    suffix = suffix or ""
    if value == nil or value ~= value or value == math.huge or value == -math.huge then
        return "n/a"
    end
    local sign = ""
    if value < 0 then
        sign = "-"
        value = -value
    end
    for index, unit in ipairs(UNITS) do
        local scaled = value / unit[1]
        local isLast = index == #UNITS
        if index == 1 then
            local rounded = math.floor(scaled + 0.5)
            if rounded < 1000 or isLast then
                return string.format("%s%d FE%s", sign, rounded, suffix)
            end
        else
            local rounded = math.floor(scaled * 100 + 0.5) / 100
            if rounded < 1000 or isLast then
                return string.format("%s%.2f %s%s", sign, scaled, unit[2], suffix)
            end
        end
    end
end

function utils.dirname(path)
    return path:match("^(.*)/[^/]*$") or "."
end

function utils.fileExists(path)
    local handle = io.open(path, "r")
    if handle then
        handle:close()
        return true
    end
    return false
end

function utils.mkdirP(dir)
    os.execute("mkdir -p '" .. dir .. "' 2>/dev/null")
end

function utils.newLogger(options)
    options = options or {}
    local logger = {
        path = options.path,
        maxBytes = options.maxBytes or 65536,
        echo = options.echo == true,
        repeatInterval = options.repeatInterval or 60,
        handle = nil,
        size = 0,
        last = {},
        lastCount = 0,
    }

    local function openFile()
        if not logger.path then
            return false
        end
        local handle = io.open(logger.path, "a")
        if not handle then
            return false
        end
        logger.handle = handle
        logger.size = handle:seek("end") or 0
        return true
    end

    local function rotate()
        if logger.handle then
            logger.handle:close()
            logger.handle = nil
        end
        os.remove(logger.path .. ".1")
        os.rename(logger.path, logger.path .. ".1")
        openFile()
    end

    function logger:write(level, message)
        local line = string.format("%s [%s] %s\n", os.date("%Y-%m-%d %H:%M:%S"), level, message)
        if not self.handle and self.path then
            openFile()
        end
        if self.handle then
            self.handle:write(line)
            self.handle:flush()
            self.size = self.size + #line
            if self.size > self.maxBytes then
                rotate()
            end
        end
        if self.echo or not self.handle then
            io.stdout:write(line)
            io.stdout:flush()
        end
    end

    function logger:info(message)
        self:write("INFO", message)
    end

    function logger:warn(message)
        self:write("WARN", message)
    end

    function logger:error(message)
        self:write("ERROR", message)
    end

    function logger:event(key, level, message)
        local now = utils.now()
        local record = self.last[key]
        if record and now - record.time < self.repeatInterval then
            record.suppressed = record.suppressed + 1
            return false
        end
        if record and record.suppressed > 0 then
            message = message .. string.format(" (+%d identiques ignores)", record.suppressed)
        end
        if not record then
            if self.lastCount >= 256 then
                self.last = {}
                self.lastCount = 0
            end
            self.lastCount = self.lastCount + 1
        end
        self.last[key] = { time = now, suppressed = 0 }
        self:write(level, message)
        return true
    end

    function logger:close()
        if self.handle then
            self.handle:close()
            self.handle = nil
        end
    end

    return logger
end

return utils
]] },
    { name = "protocol.lua", content = [=[
local cjson = require("cjson")

local protocol = {}

protocol.VERSION = 1
protocol.MAX_SIZE = 1400

protocol.TYPE_SUBSCRIBE = "subscribe"
protocol.TYPE_UNSUBSCRIBE = "unsubscribe"
protocol.TYPE_GET_SNAPSHOT = "get_snapshot"
protocol.TYPE_SNAPSHOT = "snapshot"
protocol.TYPE_ERROR = "error"

local REQUIRED_NUMBERS = {
    { "production", "j_t" },
    { "production", "fe_t" },
    { "production", "fe_s" },
    { "generators", "total" },
    { "generators", "active" },
    { "generators", "invalid" },
    { "storage", "fe" },
    { "storage", "capacity_fe" },
    { "cumulative", "fe" },
}

function protocol.encode(message)
    message.proto = protocol.VERSION
    local ok, encoded = pcall(cjson.encode, message)
    if not ok then
        return nil, tostring(encoded)
    end
    if #encoded > protocol.MAX_SIZE then
        return nil, "message_too_large"
    end
    return encoded
end

function protocol.decode(raw)
    if type(raw) ~= "string" or raw == "" then
        return nil, "empty"
    end
    local ok, message = pcall(cjson.decode, raw)
    if not ok or type(message) ~= "table" then
        return nil, "invalid_json"
    end
    if message.proto ~= protocol.VERSION then
        return nil, "unsupported_version"
    end
    if type(message.type) ~= "string" then
        return nil, "missing_type"
    end
    return message
end

function protocol.validateSnapshot(message)
    if message.type ~= protocol.TYPE_SNAPSHOT then
        return false, "not_a_snapshot"
    end
    for _, path in ipairs(REQUIRED_NUMBERS) do
        local section = message[path[1]]
        if type(section) ~= "table" or type(section[path[2]]) ~= "number" then
            return false, "missing_" .. path[1] .. "_" .. path[2]
        end
    end
    return true
end

return protocol
]=] },
    { name = "network.lua", content = [[
local socket = require("socket")
local protocol = require("protocol")

local Client = {}
Client.__index = Client

function Client.new(config, log)
    local udp, err = socket.udp()
    if not udp then
        return nil, tostring(err)
    end
    udp:settimeout(0)
    local ok, bindErr = udp:setsockname("*", 0)
    if not ok then
        udp:close()
        return nil, tostring(bindErr)
    end
    return setmetatable({
        udp = udp,
        config = config,
        log = log,
        lastError = nil,
    }, Client)
end

function Client:socket()
    return self.udp
end

function Client:subscribe()
    local data = protocol.encode({ type = protocol.TYPE_SUBSCRIBE, client = self.config.clientId })
    local ok, err = self.udp:sendto(data, self.config.centralAddress, self.config.centralPort)
    if not ok then
        self.log:event("send", "ERROR", "Erreur reseau: envoi vers le central impossible (" .. tostring(err) .. ")")
    end
    return ok
end

function Client:unsubscribe()
    local data = protocol.encode({ type = protocol.TYPE_UNSUBSCRIBE, client = self.config.clientId })
    self.udp:sendto(data, self.config.centralAddress, self.config.centralPort)
end

function Client:receive()
    local latest = nil
    for _ = 1, 32 do
        local raw, ip = self.udp:receivefrom()
        if not raw then
            break
        end
        if ip == self.config.centralAddress then
            local message, err = protocol.decode(raw)
            if not message then
                self.lastError = err
                self.log:event("decode", "WARN", "Erreur API: reponse invalide du central (" .. tostring(err) .. ")")
            elseif message.type == protocol.TYPE_ERROR then
                self.lastError = tostring(message.error)
                self.log:event("remote", "ERROR", "Le central a refuse la requete: " .. tostring(message.error))
            else
                local valid, reason = protocol.validateSnapshot(message)
                if valid then
                    latest = message
                    self.lastError = nil
                else
                    self.lastError = reason
                    self.log:event("snapshot", "WARN", "Erreur API: snapshot incomplet (" .. tostring(reason) .. ")")
                end
            end
        end
    end
    return latest
end

function Client:close()
    self.udp:close()
end

return Client
]] },
    { name = "renderer.lua", content = [=[
local utils = require("utils")

local Renderer = {}

local ESC = "\27["
local COLORS = {
    reset = ESC .. "0m",
    green = ESC .. "32;1m",
    red = ESC .. "31;1m",
    yellow = ESC .. "33;1m",
    cyan = ESC .. "36;1m",
    gray = ESC .. "1;30m",
    white = ESC .. "37;1m",
}

local function field(snapshot, section, key)
    if type(snapshot) ~= "table" then
        return nil
    end
    local data = snapshot[section]
    if type(data) ~= "table" then
        return nil
    end
    return data[key]
end

local function energy(value, suffix)
    if value == nil then
        return "--"
    end
    return utils.formatEnergy(value, suffix)
end

local function line(text, color)
    return { text = text, color = color }
end

local function center(text, width)
    if #text >= width then
        return text
    end
    return string.rep(" ", (width - #text) // 2) .. text
end

local function statusInfo(state)
    if state.online then
        return "ONLINE", "green"
    end
    if state.snapshot then
        return "OFFLINE", "red"
    end
    if state.protocolError then
        return "PROTOCOLE INCOMPATIBLE", "red"
    end
    return "CONNEXION...", "yellow"
end

local function ageText(state, now)
    if not state.lastReceive then
        return "aucune donnee recue"
    end
    return string.format("il y a %.1f s", math.max(0, now - state.lastReceive))
end

local function generatorsText(snapshot)
    local active = field(snapshot, "generators", "active")
    local total = field(snapshot, "generators", "total")
    if active == nil or total == nil then
        return "--"
    end
    return string.format("%d / %d actifs", active, total)
end

local function buildSections(state, now, width)
    local snapshot = state.snapshot
    local statusText, statusColor = statusInfo(state)
    local invalid = field(snapshot, "generators", "invalid")

    local sections = {}

    local statusLines = { line("ETAT SYSTEME", "cyan"), line(statusText, statusColor) }
    if not state.online and state.snapshot then
        statusLines[#statusLines + 1] = line("CENTRAL OFFLINE", "red")
    end
    if snapshot and snapshot.bus == "down" then
        statusLines[#statusLines + 1] = line("Bus OC2 du central indisponible", "yellow")
    end
    sections[#sections + 1] = { name = "status", priority = 0, lines = statusLines }

    sections[#sections + 1] = {
        name = "production",
        priority = 0,
        lines = {
            line("PRODUCTION ACTUELLE", "cyan"),
            line(energy(field(snapshot, "production", "fe_t"), "/t"), "white"),
            line(energy(field(snapshot, "production", "fe_s"), "/s"), "white"),
        },
    }

    local generatorLines = { line("GENERATEURS", "cyan"), line(generatorsText(snapshot), "white") }
    if invalid and invalid > 0 then
        generatorLines[#generatorLines + 1] = line(string.format("%d sans reponse", invalid), "yellow")
    end
    sections[#sections + 1] = { name = "generators", priority = 0, lines = generatorLines }

    sections[#sections + 1] = {
        name = "storage",
        priority = 0,
        lines = {
            line("ENERGIE STOCKEE", "cyan"),
            line(energy(field(snapshot, "storage", "fe")), "white"),
        },
    }

    sections[#sections + 1] = {
        name = "cumulative",
        priority = 0,
        lines = {
            line("PRODUCTION CUMULEE", "cyan"),
            line(energy(field(snapshot, "cumulative", "fe")), "white"),
        },
    }

    sections[#sections + 1] = {
        name = "averages",
        priority = 2,
        lines = {
            line("MOYENNES", "cyan"),
            line("1 min : " .. energy(field(snapshot, "stats", "avg_1m_fe_s"), "/s"), "white"),
            line("1 h   : " .. energy(field(snapshot, "stats", "avg_1h_fe_s"), "/s"), "white"),
            line("Pic   : " .. energy(field(snapshot, "stats", "peak_fe_s"), "/s"), "white"),
        },
    }

    local sourceLines = { line("SOURCES", "cyan") }
    local sources = type(snapshot) == "table" and snapshot.sources or nil
    for _, entry in ipairs({ { "wind", "Eolien" }, { "solar", "Solaire" }, { "nuclear", "Nucleaire" } }) do
        local info = type(sources) == "table" and sources[entry[1]] or nil
        local text = "--"
        local color = "gray"
        if type(info) == "table" then
            if info.enabled then
                text = string.format("%d/%d  %s", info.active or 0, info.total or 0, energy(info.fe_s, "/s"))
                color = "white"
            else
                text = "desactive"
            end
        end
        sourceLines[#sourceLines + 1] = line(entry[2] .. " : " .. text, color)
    end
    sections[#sections + 1] = { name = "sources", priority = 1, lines = sourceLines }

    local updateColor = state.online and "white" or "yellow"
    sections[#sections + 1] = {
        name = "update",
        priority = 0,
        lines = {
            line("DERNIERE ACTUALISATION", "cyan"),
            line(ageText(state, now), updateColor),
        },
    }
    return sections
end

local function assemble(sections, withSpacing, width)
    local bar = string.rep("=", width)
    local lines = {
        line(bar, "gray"),
        line(center("MEKANISM POWER MONITOR", width), "white"),
        line(bar, "gray"),
    }
    for _, section in ipairs(sections) do
        if withSpacing then
            lines[#lines + 1] = line("", nil)
        end
        for _, entry in ipairs(section.lines) do
            lines[#lines + 1] = entry
        end
    end
    if withSpacing then
        lines[#lines + 1] = line("", nil)
    end
    lines[#lines + 1] = line(bar, "gray")
    return lines
end

local function buildCompact(state, now, rows)
    local snapshot = state.snapshot
    local statusText, statusColor = statusInfo(state)
    local feT = energy(field(snapshot, "production", "fe_t"), "/t")
    local feS = energy(field(snapshot, "production", "fe_s"), "/s")
    local lines = {
        line("ETAT  " .. statusText, statusColor),
        line("PROD  " .. feT .. "  " .. feS, "white"),
        line("GEN   " .. generatorsText(snapshot) .. "  STOCK " .. energy(field(snapshot, "storage", "fe")), "white"),
        line("CUMUL " .. energy(field(snapshot, "cumulative", "fe")), "white"),
        line("MAJ   " .. ageText(state, now), state.online and "white" or "yellow"),
    }
    if rows >= #lines + 1 then
        table.insert(lines, 1, line("MEKANISM POWER MONITOR", "white"))
    end
    while #lines > rows do
        table.remove(lines)
    end
    return lines
end

function Renderer.build(state, now, columns, rows, maxWidth)
    local width = math.max(20, math.min(columns, maxWidth or 72))
    local sections = buildSections(state, now, width)

    local candidates = {}
    for priority = 2, 0, -1 do
        local subset = {}
        for _, section in ipairs(sections) do
            if section.priority <= priority then
                subset[#subset + 1] = section
            end
        end
        candidates[#candidates + 1] = subset
    end

    for _, subset in ipairs(candidates) do
        for _, spacing in ipairs({ true, false }) do
            local lines = assemble(subset, spacing, width)
            if #lines <= rows then
                return lines, width
            end
        end
    end
    return buildCompact(state, now, rows), width
end

function Renderer.toString(lines, width, columns, useColor)
    local output = {}
    for _, entry in ipairs(lines) do
        local text = entry.text
        if #text > width then
            text = text:sub(1, width)
        end
        if useColor and entry.color then
            output[#output + 1] = COLORS[entry.color] .. text .. COLORS.reset .. ESC .. "K"
        else
            output[#output + 1] = text .. ESC .. "K"
        end
    end
    return table.concat(output, "\n")
end

return Renderer
]=] },
    { name = "selftest.lua", content = [[
local scriptDir = (arg and arg[0] or ""):match("^(.*)/[^/]*$") or "."
package.path = scriptDir .. "/?.lua;" .. package.path

local utils = require("utils")
local Renderer = require("renderer")
local protocol = require("protocol")

utils.setConversion(2, 5)

local passed, failed = 0, 0

local function check(name, condition, detail)
    if condition then
        passed = passed + 1
        print("[OK]    " .. name)
    else
        failed = failed + 1
        print("[ECHEC] " .. name .. (detail and (" -> " .. tostring(detail)) or ""))
    end
end

local function snapshot(feSecond)
    return {
        type = "snapshot",
        status = "online",
        bus = "ok",
        production = { j_t = feSecond / 8, fe_t = feSecond / 20, fe_s = feSecond },
        generators = { total = 30, active = 24, invalid = 0, stale = 0 },
        storage = { fe = 1250000, capacity_fe = 5000000 },
        cumulative = { fe = 842730000000 },
        stats = { avg_1m_fe_s = feSecond, avg_1h_fe_s = feSecond, peak_fe_s = feSecond },
        sources = { wind = { enabled = true, total = 30, active = 24, fe_t = 1, fe_s = 20 },
            solar = { enabled = false }, nuclear = { enabled = false } },
    }
end

local function joined(lines)
    local parts = {}
    for _, entry in ipairs(lines) do
        parts[#parts + 1] = entry.text
    end
    return table.concat(parts, "\n")
end

local online = { snapshot = snapshot(249000), lastReceive = 100, online = true }
local lines, width = Renderer.build(online, 100.4, 80, 30, 72)
local text = joined(lines)
check("Ecran: titre", text:find("MEKANISM POWER MONITOR", 1, true) ~= nil)
check("Ecran: ONLINE", text:find("ONLINE", 1, true) ~= nil and text:find("OFFLINE", 1, true) == nil)
check("Ecran: production kFE", text:find("249.00 kFE/s", 1, true) ~= nil, text)
check("Ecran: generateurs", text:find("24 / 30 actifs", 1, true) ~= nil)
check("Ecran: cumul GFE", text:find("842.73 GFE", 1, true) ~= nil)
check("Ecran: age", text:find("il y a 0.4 s", 1, true) ~= nil)
check("Ecran: solaire desactive", text:find("Solaire : desactive", 1, true) ~= nil)
check("Ecran: largeur respectee", width <= 72)

local offline = { snapshot = snapshot(249000), lastReceive = 100, online = false }
lines = Renderer.build(offline, 130, 80, 30, 72)
text = joined(lines)
check("Ecran: CENTRAL OFFLINE", text:find("CENTRAL OFFLINE", 1, true) ~= nil)

local waiting = { snapshot = nil, lastReceive = nil, online = false }
lines = Renderer.build(waiting, 1, 80, 30, 72)
text = joined(lines)
check("Ecran: attente sans donnees", text:find("CONNEXION...", 1, true) ~= nil and text:find("--", 1, true) ~= nil)

for _, size in ipairs({ { 24, 80 }, { 18, 60 }, { 12, 40 }, { 6, 30 }, { 4, 20 } }) do
    local rows, columns = size[1], size[2]
    local built, builtWidth = Renderer.build(online, 100.4, columns, rows, 72)
    local rendered = Renderer.toString(built, builtWidth, columns, true)
    local count = select(2, rendered:gsub("\n", "\n")) + 1
    check(string.format("Ecran %dx%d: tient dans la taille (%d lignes)", columns, rows, count),
        count <= rows and builtWidth <= columns, count)
end

for _, value in ipairs({ 950, 15400, 2.8e6, 4.2e9, 1.1e12 }) do
    lines = Renderer.build({ snapshot = snapshot(value), lastReceive = 1, online = true }, 1, 80, 30, 72)
    check("Ecran unite pour " .. value, joined(lines):find("FE/s", 1, true) ~= nil)
end

print(string.format("\n%d reussis, %d echecs", passed, failed))
os.exit(failed == 0 and 0 or 1)
]] },
    { name = "run_screen.sh", content = [[
#!/bin/sh

export PATH="/bin:/sbin:/usr/bin:/usr/sbin"
export TERM=linux

lua /root/energy_monitor/screen/main.lua
sleep 2
]] },
}

local targetDir = DEFAULT_DIR
local autostart = false
for _, value in ipairs(arg) do
    if value == "--autostart" then
        autostart = true
    else
        targetDir = value
    end
end

local function quote(path)
    return "'" .. path .. "'"
end

local function readAll(path)
    local handle = io.open(path, "rb")
    if not handle then
        return nil
    end
    local content = handle:read("a")
    handle:close()
    return content
end

os.execute("mkdir -p " .. quote(targetDir) .. " " .. quote(targetDir .. "/data"))
if ROLE == "central" then
    os.execute("mkdir -p " .. quote(targetDir .. "/collectors"))
end

local written, kept = 0, 0
for _, file in ipairs(files) do
    local path = targetDir .. "/" .. file.name
    local existing = readAll(path)
    if existing and KEEP_IF_EXISTS[file.name] then
        kept = kept + 1
        if existing ~= file.content then
            local alt = io.open(path .. ".new", "wb")
            if alt then
                alt:write(file.content)
                alt:close()
            end
            print("Configuration existante conservee: " .. path .. " (nouvelle version dans " .. file.name .. ".new)")
        end
    else
        local handle, err = io.open(path, "wb")
        if not handle then
            io.stderr:write("Ecriture impossible: " .. path .. " (" .. tostring(err) .. ")\n")
            os.exit(1)
        end
        handle:write(file.content)
        handle:close()
        local check = readAll(path)
        if check ~= file.content then
            io.stderr:write("Verification echouee: " .. path .. "\n")
            os.exit(1)
        end
        written = written + 1
    end
end

for _, name in ipairs(EXECUTABLE) do
    os.execute("chmod +x " .. quote(targetDir .. "/" .. name))
end

print(string.format("Installation %s terminee dans %s (%d fichiers ecrits, %d conserves)", ROLE, targetDir, written, kept))

if autostart then
    if ROLE ~= "central" then
        print("--autostart ne concerne que le central. Voir INSTALL.md pour l'ecran.")
    else
        local source = targetDir .. "/S99z_energy_central"
        os.execute("cp " .. quote(source) .. " /etc/init.d/S99z_energy_central && chmod +x /etc/init.d/S99z_energy_central")
        print("Demarrage automatique installe: /etc/init.d/S99z_energy_central")
    end
end
