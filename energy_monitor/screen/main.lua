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
