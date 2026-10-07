local scriptDir = (arg and arg[0] or ""):match("^(.*)/[^/]*$") or "."
package.path = scriptDir .. "/?.lua;" .. package.path
    .. ";/run/oc2/lib/lua/?.lua;/run/oc2/lib/lua/?/init.lua"
    .. ";/mnt/scripts/lib/lua/?.lua;/mnt/scripts/lib/lua/?/init.lua"

local socket = require("socket")
local config = require("config")
local utils = require("utils")
local protocol = require("protocol")
local collectors = require("collectors")
local Discovery = require("discovery")
local Aggregator = require("aggregator")
local Persistence = require("persistence")
local Network = require("network")

local USAGE = [[
Usage: lua main.lua [options]
  --verbose              recopie les logs sur la sortie standard
  --data-dir <dossier>   dossier des donnees (defaut: <programme>/data)
  --fake-bus <n>         TEST UNIQUEMENT: n generateurs simules au lieu du bus OC2
  --fake-j <j/t>         TEST UNIQUEMENT: production simulee par generateur (defaut 400)
  --fake-latency <s>     TEST UNIQUEMENT: latence simulee par appel (defaut 0)
]]

local function parseArguments(args)
    local options = {}
    local index = 1
    while args[index] do
        local name = args[index]
        if name == "--verbose" then
            options.verbose = true
            index = index + 1
        elseif name == "--data-dir" then
            options.dataDir = args[index + 1]
            index = index + 2
        elseif name == "--fake-bus" then
            options.fakeCount = tonumber(args[index + 1])
            index = index + 2
        elseif name == "--fake-j" then
            options.fakeJ = tonumber(args[index + 1])
            index = index + 2
        elseif name == "--fake-latency" then
            options.fakeLatency = tonumber(args[index + 1])
            index = index + 2
        else
            return nil
        end
    end
    if options.dataDir == "" or (options.fakeCount ~= nil and options.fakeCount < 0) then
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

local function run(options)
    utils.setConversion(config.energy.feNumerator, config.energy.feDenominator)

    local dataDir = options.dataDir or config.storage.dataDir or (scriptDir .. "/data")
    utils.mkdirP(dataDir)
    local log = utils.newLogger({
        path = dataDir .. "/" .. config.log.fileName,
        maxBytes = config.log.maxBytes,
        repeatInterval = config.log.repeatInterval,
        echo = options.verbose == true,
    })

    local isFake = options.fakeCount ~= nil
    log:info("Central demarre" .. (isFake and " (MODE TEST: bus simule)" or ""))

    local specs, sourcesInfo = collectors.build(config.sources, log)

    local persistence = Persistence.new({
        dir = dataDir,
        log = log,
        fileName = isFake and "production_fake.dat" or config.storage.fileName,
        saveInterval = config.storage.saveInterval,
    })
    local cumulativeJoules = persistence:load()

    local aggregator = Aggregator.new({
        log = log,
        sourcesInfo = sourcesInfo,
        staleAfter = config.staleAfter,
        maxFailures = config.maxConsecutiveFailures,
        ticksPerSecond = config.ticksPerSecond,
        maxDt = config.updateInterval * 3,
        cumulativeJoules = cumulativeJoules,
    })

    local connect = nil
    if isFake then
        local FakeBus = require("fakebus")
        local fakeBus = FakeBus.new(options.fakeCount, options.fakeJ or 400, options.fakeLatency)
        connect = function()
            return fakeBus
        end
    end
    local discovery = Discovery.new({
        connect = connect,
        log = log,
        reconnectDelay = config.busReconnectDelay,
    })

    local network = nil
    if config.api.enabled then
        local created, err = Network.new(config.api, log)
        if not created then
            log:error("Erreur reseau: API non demarree (" .. tostring(err) .. ")")
            log:close()
            io.stderr:write("API non demarree: " .. tostring(err) .. "\n")
            return 1
        end
        network = created
        log:info(string.format("API interne demarree sur %s:%d (protocole %d)",
            config.api.bindAddress, config.api.port, protocol.VERSION))
    end

    local startTime = utils.now()
    local lastCycle = startTime
    local nextDiscovery = 0
    local sequence = 0
    local lastEncoded = nil
    local forceDiscovery = false

    local function getSnapshot()
        return lastEncoded
    end

    while not utils.shouldStop() do
        local cycleStart = utils.now()
        local ok, err = pcall(function()
            local dt = cycleStart - lastCycle
            lastCycle = cycleStart

            if forceDiscovery or cycleStart >= nextDiscovery or not discovery.bus then
                local list = discovery:scan()
                if list then
                    local found = collectors.classify(list, specs)
                    aggregator:sync(found, cycleStart)
                    nextDiscovery = cycleStart + config.discoveryInterval
                    forceDiscovery = false
                else
                    nextDiscovery = cycleStart + 1
                end
            end

            if discovery.bus then
                local polled, failed, seconds = collectors.pollRound(
                    discovery.bus, aggregator, specs, config.pollBudget)
                aggregator.poll = { polled = polled, failed = failed, seconds = seconds }
                if polled > 0 and failed == polled then
                    forceDiscovery = true
                end
            end

            aggregator:compute(cycleStart, dt)
            sequence = sequence + 1
            local snapshot = aggregator:snapshot({
                seq = sequence,
                uptime = cycleStart - startTime,
                busOk = discovery.bus ~= nil,
            })
            local encoded, encodeErr = protocol.encode(snapshot)
            if encoded then
                lastEncoded = encoded
                if network then
                    network:broadcast(encoded)
                end
            else
                log:event("encode", "ERROR", "Erreur API: snapshot non encode (" .. tostring(encodeErr) .. ")")
            end
            persistence:saveIfDue(aggregator.cumulativeJoules)
        end)
        if not ok then
            log:event("cycle", "ERROR", "Erreur interne: " .. tostring(err))
        end

        local deadline = cycleStart + config.updateInterval
        while not utils.shouldStop() do
            local remaining = deadline - utils.now()
            if remaining <= 0 then
                break
            end
            if network then
                socket.select({ network:socket() }, nil, math.min(remaining, 0.25))
                network:pump(getSnapshot)
            else
                socket.sleep(math.min(remaining, 0.25))
            end
        end
    end

    persistence:save(aggregator.cumulativeJoules)
    log:info("Central arrete")
    if network then
        network:close()
    end
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
