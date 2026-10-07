local ROLE = "central"
local DEFAULT_DIR = "/root/energy_monitor/central"
local EXECUTABLE = {"S99z_energy_central"}
local KEEP_IF_EXISTS = { ["config.lua"] = true }

local files = {
    { name = "main.lua", content = [=[
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
]=] },
    { name = "config.lua", content = [[
return {
    updateInterval = 1.0,
    discoveryInterval = 10.0,
    busReconnectDelay = 5.0,
    pollBudget = 0.6,
    staleAfter = 10.0,
    maxConsecutiveFailures = 3,
    ticksPerSecond = 20,

    sources = {
        wind = true,
        solar = false,
        nuclear = false,
    },

    energy = {
        feNumerator = 2,
        feDenominator = 5,
    },

    api = {
        enabled = true,
        bindAddress = "*",
        port = 4710,
        subscriberTtl = 15,
        maxSubscribers = 16,
        maxDatagram = 2048,
    },

    storage = {
        dataDir = nil,
        fileName = "production.dat",
        saveInterval = 30,
    },

    log = {
        fileName = "central.log",
        maxBytes = 65536,
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
    { name = "discovery.lua", content = [[
local utils = require("utils")

local Discovery = {}
Discovery.__index = Discovery

local function defaultConnect()
    local loaded, busModule = pcall(require, "oc2.bus")
    if not loaded then
        return nil, "bibliotheque oc2.bus introuvable: " .. tostring(busModule)
    end
    local ok, bus, reason = pcall(busModule.connect)
    if not ok then
        return nil, tostring(bus)
    end
    if not bus then
        return nil, tostring(reason)
    end
    return bus
end

function Discovery.new(options)
    return setmetatable({
        connect = options.connect or defaultConnect,
        log = options.log,
        reconnectDelay = options.reconnectDelay,
        bus = nil,
        nextAttempt = 0,
        everConnected = false,
        wasDown = false,
    }, Discovery)
end

function Discovery:ensureBus()
    if self.bus then
        return self.bus
    end
    local now = utils.now()
    if now < self.nextAttempt then
        return nil
    end
    local bus, reason = self.connect()
    if not bus then
        self.nextAttempt = now + self.reconnectDelay
        self.log:event("bus:connect", "ERROR", "Erreur reseau: bus OC2 inaccessible (" .. tostring(reason) .. ")")
        self.wasDown = true
        return nil
    end
    self.bus = bus
    if self.everConnected or self.wasDown then
        self.log:info("Reconnexion au bus OC2 reussie")
    else
        self.log:info("Bus OC2 connecte")
    end
    self.everConnected = true
    self.wasDown = false
    return bus
end

function Discovery:dropBus(reason)
    if self.bus then
        pcall(function()
            if self.bus.close then
                self.bus:close()
            end
        end)
        self.bus = nil
    end
    self.wasDown = true
    self.nextAttempt = utils.now() + self.reconnectDelay
    self.log:event("bus:drop", "ERROR", "Erreur reseau: connexion au bus perdue (" .. tostring(reason) .. ")")
end

function Discovery:scan()
    local bus = self:ensureBus()
    if not bus then
        return nil, "bus_down"
    end
    local ok, list = pcall(bus.list, bus)
    if not ok or type(list) ~= "table" then
        self:dropBus(list)
        return nil, "list_failed"
    end
    return list
end

return Discovery
]] },
    { name = "collectors.lua", content = [[
local utils = require("utils")

local collectors = {}

local SOURCE_ORDER = { "wind", "solar", "nuclear" }

local function asNumber(value)
    if type(value) ~= "number" or value ~= value or value == math.huge or value == -math.huge then
        return nil
    end
    return value
end

local function asInteger(value)
    value = asNumber(value)
    if value == nil then
        return nil
    end
    return math.floor(value)
end

function collectors.parseGeneratorData(data)
    if type(data) ~= "table" then
        return nil, "bad_reply"
    end
    if data.valid ~= true then
        return nil, "device_error:" .. tostring(data.error)
    end
    local production = asInteger(data.production_j_t)
    if production == nil then
        return nil, "bad_production"
    end
    local x, y, z = asInteger(data.x), asInteger(data.y), asInteger(data.z)
    local position = nil
    if x and y and z and type(data.dimension) == "string" then
        position = string.format("%s:%d:%d:%d", data.dimension, x, y, z)
    end
    return {
        active = data.active == true,
        jPerTick = math.max(0, production),
        maxJPerTick = math.max(0, asInteger(data.max_output_j_t) or 0),
        storedJ = math.max(0, asInteger(data.stored_j) or 0),
        capacityJ = math.max(0, asInteger(data.capacity_j) or 0),
        position = position,
    }
end

function collectors.build(sourcesConfig, log)
    local specs = {}
    local info = {}
    for _, name in ipairs(SOURCE_ORDER) do
        local enabled = sourcesConfig[name] == true
        local entry = { enabled = enabled, available = false }
        if enabled then
            local ok, spec = pcall(require, "collectors." .. name)
            if ok and type(spec) == "table" then
                specs[name] = spec
                entry.available = true
            else
                log:warn("Source " .. name .. " activee mais aucun collecteur n'est installe")
            end
        end
        info[name] = entry
    end
    return specs, info
end

function collectors.classify(list, specs)
    local found = {}
    local count = 0
    for _, device in ipairs(list) do
        if type(device) == "table" and type(device.deviceId) == "string" and type(device.typeNames) == "table" then
            local matched = nil
            for _, name in ipairs(SOURCE_ORDER) do
                local spec = specs[name]
                if spec then
                    for _, typeName in ipairs(device.typeNames) do
                        if typeName == spec.typeName then
                            matched = name
                            break
                        end
                    end
                end
                if matched then
                    break
                end
            end
            if matched then
                found[device.deviceId] = matched
                count = count + 1
            end
        end
    end
    return found, count
end

local function readOne(bus, deviceId, spec)
    local data = bus:invoke(deviceId, spec.method)
    return collectors.parseGeneratorData(data)
end

function collectors.pollRound(bus, store, specs, budget)
    local total = store:count()
    local startTime = utils.now()
    local polled, failed = 0, 0
    while polled < total do
        local deviceId, source = store:nextId()
        if not deviceId then
            break
        end
        local spec = specs[source]
        local ok, record, err = pcall(readOne, bus, deviceId, spec)
        local now = utils.now()
        if ok and record then
            store:update(deviceId, record, now)
        else
            failed = failed + 1
            store:fail(deviceId, ok and err or record, now)
        end
        polled = polled + 1
        if now - startTime >= budget then
            break
        end
    end
    return polled, failed, utils.now() - startTime
end

return collectors
]] },
    { name = "collectors/wind.lua", content = [[
return {
    name = "wind",
    typeName = "mekanism_wind_generator",
    method = "getGeneratorData",
}
]] },
    { name = "aggregator.lua", content = [[
local utils = require("utils")

local Window = {}
Window.__index = Window

function Window.new(count, bucketLength)
    local self = setmetatable({
        count = count,
        bucketLength = bucketLength,
        joules = {},
        seconds = {},
        head = 1,
        currentJoules = 0,
        currentSeconds = 0,
    }, Window)
    for index = 1, count do
        self.joules[index] = 0
        self.seconds[index] = 0
    end
    return self
end

function Window:add(joules, seconds)
    self.currentJoules = self.currentJoules + joules
    self.currentSeconds = self.currentSeconds + seconds
    if self.currentSeconds >= self.bucketLength then
        self.joules[self.head] = self.currentJoules
        self.seconds[self.head] = self.currentSeconds
        self.head = self.head % self.count + 1
        self.currentJoules = 0
        self.currentSeconds = 0
    end
end

function Window:totals()
    local joules, seconds = self.currentJoules, self.currentSeconds
    for index = 1, self.count do
        joules = joules + self.joules[index]
        seconds = seconds + self.seconds[index]
    end
    return joules, seconds
end

local Aggregator = {}
Aggregator.__index = Aggregator

function Aggregator.new(options)
    return setmetatable({
        log = options.log,
        sourcesInfo = options.sourcesInfo,
        staleAfter = options.staleAfter,
        maxFailures = options.maxFailures,
        ticksPerSecond = options.ticksPerSecond,
        maxDt = options.maxDt,
        cumulativeJoules = options.cumulativeJoules or 0,
        devices = {},
        order = {},
        cursor = 1,
        deviceCount = 0,
        best = {},
        previousJPerTick = nil,
        peakFeSecond = 0,
        minute = Window.new(60, 1),
        hour = Window.new(60, 60),
        totals = nil,
        poll = { polled = 0, failed = 0, seconds = 0 },
    }, Aggregator)
end

function Aggregator:count()
    return self.deviceCount
end

function Aggregator:sync(found, now)
    local added, removed = {}, {}
    for deviceId, source in pairs(found) do
        if not self.devices[deviceId] then
            self.devices[deviceId] = {
                id = deviceId,
                source = source,
                record = nil,
                lastOk = 0,
                failures = 0,
                firstSeen = now,
            }
            self.deviceCount = self.deviceCount + 1
            added[#added + 1] = deviceId
        end
    end
    for deviceId in pairs(self.devices) do
        if not found[deviceId] then
            self.devices[deviceId] = nil
            self.deviceCount = self.deviceCount - 1
            removed[#removed + 1] = deviceId
        end
    end
    if #added > 0 or #removed > 0 then
        local order = {}
        for deviceId in pairs(self.devices) do
            order[#order + 1] = deviceId
        end
        table.sort(order)
        self.order = order
        if self.cursor > #order then
            self.cursor = 1
        end
        self:logChanges("ajoute", added)
        self:logChanges("supprime", removed)
    end
    return #added, #removed
end

function Aggregator:logChanges(verb, ids)
    if #ids == 0 then
        return
    end
    if #ids <= 5 then
        table.sort(ids)
        for _, deviceId in ipairs(ids) do
            self.log:info("Generateur " .. verb .. ": " .. deviceId)
        end
    else
        self.log:info(string.format("%d generateurs %ss", #ids, verb))
    end
end

function Aggregator:nextId()
    local total = #self.order
    if total == 0 then
        return nil
    end
    if self.cursor > total then
        self.cursor = 1
    end
    local deviceId = self.order[self.cursor]
    self.cursor = self.cursor % total + 1
    local entry = self.devices[deviceId]
    return deviceId, entry and entry.source
end

function Aggregator:update(deviceId, record, now)
    local entry = self.devices[deviceId]
    if not entry then
        return
    end
    entry.record = record
    entry.lastOk = now
    entry.failures = 0
end

function Aggregator:fail(deviceId, reason, now)
    local entry = self.devices[deviceId]
    if not entry then
        return
    end
    entry.failures = entry.failures + 1
    if entry.failures >= self.maxFailures then
        if entry.record then
            self.log:event("gen:" .. deviceId, "ERROR",
                "Erreur generateur " .. deviceId .. ": " .. tostring(reason))
        end
        entry.record = nil
    end
end

function Aggregator:compute(now, dt)
    local best = self.best
    for key in pairs(best) do
        best[key] = nil
    end
    local withPosition = 0
    for deviceId, entry in pairs(self.devices) do
        local record = entry.record
        if record and record.position then
            withPosition = withPosition + 1
            local current = best[record.position]
            if not current or entry.lastOk > current.lastOk
                or (entry.lastOk == current.lastOk and deviceId < current.id) then
                best[record.position] = entry
            end
        end
    end

    local total, active, invalid, stale = 0, 0, 0, 0
    local joules, stored, capacity = 0, 0, 0
    local perSource = {}
    for name in pairs(self.sourcesInfo) do
        perSource[name] = { total = 0, active = 0, joules = 0 }
    end
    local uniquePositions = 0
    for _ in pairs(best) do
        uniquePositions = uniquePositions + 1
    end

    for _, entry in pairs(self.devices) do
        local record = entry.record
        local isDuplicate = record and record.position and best[record.position] ~= entry
        if not isDuplicate then
            total = total + 1
            local source = perSource[entry.source]
            if source then
                source.total = source.total + 1
            end
            if record and (now - entry.lastOk) <= self.staleAfter then
                if record.active then
                    active = active + 1
                    if source then
                        source.active = source.active + 1
                    end
                end
                joules = joules + record.jPerTick
                stored = stored + record.storedJ
                capacity = capacity + record.capacityJ
                if source then
                    source.joules = source.joules + record.jPerTick
                end
            else
                invalid = invalid + 1
                if record then
                    stale = stale + 1
                end
            end
        end
    end

    if withPosition > uniquePositions then
        self.log:event("dup", "WARN", string.format(
            "%d peripheriques designent un generateur deja vu: comptes une seule fois",
            withPosition - uniquePositions))
    end

    if dt > self.maxDt then
        self.log:event("dt", "WARN", string.format("Intervalle de %.2f s plafonne a %.2f s", dt, self.maxDt))
        dt = self.maxDt
    end
    local average = joules
    if self.previousJPerTick then
        average = (self.previousJPerTick + joules) / 2
    end
    self.previousJPerTick = joules
    local produced = average * self.ticksPerSecond * dt
    self.cumulativeJoules = self.cumulativeJoules + produced
    self.minute:add(produced, dt)
    self.hour:add(produced, dt)

    local feSecond = utils.joulesToFeExact(joules) * self.ticksPerSecond
    if feSecond > self.peakFeSecond then
        self.peakFeSecond = feSecond
    end

    self.totals = {
        total = total,
        active = active,
        invalid = invalid,
        stale = stale,
        joules = joules,
        stored = stored,
        capacity = capacity,
        perSource = perSource,
    }
    return self.totals
end

local function windowStats(window)
    local joules, seconds = window:totals()
    local producedFe = utils.joulesToFeExact(joules)
    local averageFe = 0
    if seconds > 0 then
        averageFe = producedFe / seconds
    end
    return utils.round(producedFe), utils.round(averageFe)
end

function Aggregator:snapshot(context)
    local totals = self.totals
    local joules = totals.joules
    local sources = {}
    for name, info in pairs(self.sourcesInfo) do
        if info.enabled and info.available then
            local stats = totals.perSource[name]
            sources[name] = {
                enabled = true,
                total = stats.total,
                active = stats.active,
                fe_t = utils.joulesToFe(stats.joules),
                fe_s = utils.round(utils.joulesToFeExact(stats.joules) * self.ticksPerSecond),
            }
        else
            sources[name] = { enabled = false }
        end
    end
    local producedMinute, averageMinute = windowStats(self.minute)
    local producedHour, averageHour = windowStats(self.hour)
    return {
        type = "snapshot",
        seq = context.seq,
        ts = os.time(),
        uptime = utils.round(context.uptime),
        status = "online",
        bus = context.busOk and "ok" or "down",
        production = {
            j_t = joules,
            fe_t = utils.joulesToFe(joules),
            fe_s = utils.round(utils.joulesToFeExact(joules) * self.ticksPerSecond),
        },
        generators = {
            total = totals.total,
            active = totals.active,
            invalid = totals.invalid,
            stale = totals.stale,
        },
        storage = {
            fe = utils.joulesToFe(totals.stored),
            capacity_fe = utils.joulesToFe(totals.capacity),
        },
        cumulative = {
            fe = utils.round(utils.joulesToFeExact(self.cumulativeJoules)),
        },
        stats = {
            produced_1m_fe = producedMinute,
            avg_1m_fe_s = averageMinute,
            produced_1h_fe = producedHour,
            avg_1h_fe_s = averageHour,
            peak_fe_s = utils.round(self.peakFeSecond),
        },
        sources = sources,
        poll = {
            polled = self.poll.polled,
            failed = self.poll.failed,
            ms = utils.round(self.poll.seconds * 1000),
        },
    }
end

Aggregator.Window = Window

return Aggregator
]] },
    { name = "persistence.lua", content = [[
local utils = require("utils")

local Persistence = {}
Persistence.__index = Persistence

local function parse(path)
    local handle = io.open(path, "r")
    if not handle then
        return nil, "absent"
    end
    local content = handle:read("a")
    handle:close()
    if not content or content:sub(-4) ~= "end\n" then
        return nil, "incomplete"
    end
    if not content:match("^version=1\n") then
        return nil, "unknown_version"
    end
    local joules = tonumber(content:match("\ncumulative_j=([%d%.]+)\n"))
    if not joules or joules < 0 or joules ~= joules then
        return nil, "invalid_value"
    end
    return joules
end

function Persistence.new(options)
    utils.mkdirP(options.dir)
    local path = options.dir .. "/" .. options.fileName
    return setmetatable({
        path = path,
        tmpPath = path .. ".tmp",
        bakPath = path .. ".bak",
        log = options.log,
        interval = options.saveInterval,
        lastSave = utils.now(),
    }, Persistence)
end

function Persistence:load()
    local best, source = nil, nil
    local damaged = false
    for _, candidate in ipairs({ self.path, self.tmpPath, self.bakPath }) do
        local joules, reason = parse(candidate)
        if joules then
            if not best or joules > best then
                best, source = joules, candidate
            end
        elseif reason ~= "absent" then
            damaged = true
            self.log:warn("Fichier de compteur inutilisable (" .. reason .. "): " .. candidate)
        end
    end
    if best then
        self.log:info(string.format("Compteur cumule restaure depuis %s", source))
        return best
    end
    if damaged then
        self.log:error("Aucun fichier de compteur valide: demarrage a 0")
    else
        self.log:info("Aucun compteur existant: demarrage a 0")
    end
    return 0
end

function Persistence:save(cumulativeJoules)
    local handle, err = io.open(self.tmpPath, "w")
    if not handle then
        self.log:event("save", "ERROR", "Sauvegarde impossible: " .. tostring(err))
        return false
    end
    handle:write(string.format("version=1\ncumulative_j=%.3f\nsaved_at=%d\nend\n",
        cumulativeJoules, os.time()))
    handle:flush()
    handle:close()
    os.execute("sync")
    os.remove(self.bakPath)
    os.rename(self.path, self.bakPath)
    local ok, renameErr = os.rename(self.tmpPath, self.path)
    if not ok then
        self.log:event("save", "ERROR", "Renommage du compteur impossible: " .. tostring(renameErr))
        return false
    end
    self.lastSave = utils.now()
    return true
end

function Persistence:saveIfDue(cumulativeJoules)
    if utils.now() - self.lastSave >= self.interval then
        return self:save(cumulativeJoules)
    end
    return true
end

return Persistence
]] },
    { name = "network.lua", content = [[
local socket = require("socket")
local protocol = require("protocol")
local utils = require("utils")

local Network = {}
Network.__index = Network

function Network.new(config, log)
    local udp, err = socket.udp()
    if not udp then
        return nil, tostring(err)
    end
    udp:settimeout(0)
    local ok, bindErr = udp:setsockname(config.bindAddress, config.port)
    if not ok then
        udp:close()
        return nil, tostring(bindErr)
    end
    return setmetatable({
        udp = udp,
        config = config,
        log = log,
        subscribers = {},
        subscriberCount = 0,
    }, Network)
end

function Network:socket()
    return self.udp
end

function Network:send(data, ip, port)
    local ok, err = self.udp:sendto(data, ip, port)
    if not ok then
        self.log:event("send:" .. ip, "ERROR", string.format("Erreur reseau: envoi vers %s:%d (%s)", ip, port, tostring(err)))
    end
    return ok
end

function Network:sendError(ip, port, reason)
    local encoded = protocol.encode({ type = protocol.TYPE_ERROR, error = reason, supported = protocol.VERSION })
    if encoded then
        self:send(encoded, ip, port)
    end
end

function Network:removeSubscriber(key, reason)
    local subscriber = self.subscribers[key]
    if subscriber then
        self.subscribers[key] = nil
        self.subscriberCount = self.subscriberCount - 1
        self.log:info(string.format("PC ecran deconnecte: %s (%s)", subscriber.client, reason))
    end
end

function Network:handle(raw, ip, port, getSnapshot)
    if #raw > self.config.maxDatagram then
        self.log:event("big:" .. ip, "WARN", "Datagramme trop gros ignore depuis " .. ip)
        return
    end
    local message, err = protocol.decode(raw)
    if not message then
        self.log:event("bad:" .. ip, "WARN", string.format("Erreur API: message invalide de %s (%s)", ip, tostring(err)))
        if err == "unsupported_version" then
            self:sendError(ip, port, err)
        end
        return
    end
    local key = ip .. ":" .. port
    local now = utils.now()
    if message.type == protocol.TYPE_SUBSCRIBE then
        local subscriber = self.subscribers[key]
        local isNew = subscriber == nil
        if isNew then
            if self.subscriberCount >= self.config.maxSubscribers then
                self.log:event("full", "WARN", "Erreur API: trop d'abonnes, " .. key .. " refuse")
                self:sendError(ip, port, "too_many_subscribers")
                return
            end
            subscriber = { ip = ip, port = port, client = tostring(message.client or "?"):sub(1, 32) }
            self.subscribers[key] = subscriber
            self.subscriberCount = self.subscriberCount + 1
            self.log:info(string.format("PC ecran connecte: %s (%s)", subscriber.client, key))
        end
        subscriber.lastSeen = now
        if isNew then
            local snapshot = getSnapshot()
            if snapshot then
                self:send(snapshot, ip, port)
            end
        end
    elseif message.type == protocol.TYPE_UNSUBSCRIBE then
        self:removeSubscriber(key, "demande du client")
    elseif message.type == protocol.TYPE_GET_SNAPSHOT then
        local snapshot = getSnapshot()
        if snapshot then
            self:send(snapshot, ip, port)
        end
    else
        self.log:event("type:" .. tostring(message.type), "WARN",
            "Erreur API: type de message inconnu '" .. tostring(message.type) .. "'")
        self:sendError(ip, port, "unknown_type")
    end
end

function Network:pump(getSnapshot)
    for _ = 1, 64 do
        local raw, ip, port = self.udp:receivefrom()
        if not raw then
            break
        end
        local ok, err = pcall(self.handle, self, raw, ip, port, getSnapshot)
        if not ok then
            self.log:event("handler", "ERROR", "Erreur API: " .. tostring(err))
        end
    end
end

function Network:broadcast(data)
    local now = utils.now()
    for key, subscriber in pairs(self.subscribers) do
        if now - subscriber.lastSeen > self.config.subscriberTtl then
            self:removeSubscriber(key, "timeout")
        else
            self:send(data, subscriber.ip, subscriber.port)
        end
    end
end

function Network:close()
    self.udp:close()
end

return Network
]] },
    { name = "fakebus.lua", content = [[
local socket = require("socket")

local FakeBus = {}
FakeBus.__index = FakeBus

function FakeBus.new(count, jPerTick, latency)
    local self = setmetatable({
        generators = {},
        nextIndex = 1,
        jPerTick = jPerTick,
        latency = latency or 0,
        calls = 0,
    }, FakeBus)
    for _ = 1, count do
        self:add()
    end
    return self
end

function FakeBus:add()
    local index = self.nextIndex
    self.nextIndex = index + 1
    self.generators[index] = {
        id = string.format("fake-%04d", index),
        active = true,
        jPerTick = self.jPerTick,
        x = index,
        y = 64,
        z = 0,
    }
    return index
end

function FakeBus:remove(index)
    self.generators[index] = nil
end

function FakeBus:setActive(index, active)
    self.generators[index].active = active
end

function FakeBus:setProduction(jPerTick)
    self.jPerTick = jPerTick
    for _, generator in pairs(self.generators) do
        generator.jPerTick = jPerTick
    end
end

function FakeBus:list()
    local result = {}
    for _, generator in pairs(self.generators) do
        result[#result + 1] = {
            deviceId = generator.id,
            typeNames = { "mekanism_wind_generator", "mekanism_generator" },
        }
    end
    table.sort(result, function(a, b)
        return a.deviceId < b.deviceId
    end)
    return result
end

function FakeBus:invoke(deviceId, methodName)
    self.calls = self.calls + 1
    if self.latency > 0 then
        socket.sleep(self.latency)
    end
    if methodName ~= "getGeneratorData" then
        error("unknown method " .. tostring(methodName), 0)
    end
    for _, generator in pairs(self.generators) do
        if generator.id == deviceId then
            local production = generator.active and generator.jPerTick or 0
            return {
                valid = true,
                type = "wind_generator",
                active = generator.active,
                production_fe_t = math.floor(production * 2 / 5 + 0.5),
                production_fe_s = math.floor(production * 2 / 5 + 0.5) * 20,
                production_j_t = production,
                max_output_fe_t = 0,
                max_output_j_t = 0,
                stored_fe = 0,
                stored_j = production,
                capacity_fe = 0,
                capacity_j = production * 100,
                x = generator.x,
                y = generator.y,
                z = generator.z,
                dimension = "minecraft:overworld",
            }
        end
    end
    error("no device with id " .. tostring(deviceId), 0)
end

function FakeBus:close()
end

return FakeBus
]] },
    { name = "selftest.lua", content = [[
local scriptDir = (arg and arg[0] or ""):match("^(.*)/[^/]*$") or "."
package.path = scriptDir .. "/?.lua;" .. package.path

local utils = require("utils")
local protocol = require("protocol")
local collectors = require("collectors")
local Aggregator = require("aggregator")
local Persistence = require("persistence")
local FakeBus = require("fakebus")
local config = require("config")

utils.setConversion(config.energy.feNumerator, config.energy.feDenominator)

local tmpDir = os.tmpname()
os.remove(tmpDir)
tmpDir = tmpDir .. "_energy_selftest"
utils.mkdirP(tmpDir)
local log = utils.newLogger({ path = tmpDir .. "/selftest.log", echo = false })

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

local specs, sourcesInfo = collectors.build({ wind = true, solar = false, nuclear = false }, log)

local function newAggregator(cumulative)
    return Aggregator.new({
        log = log,
        sourcesInfo = sourcesInfo,
        staleAfter = config.staleAfter,
        maxFailures = config.maxConsecutiveFailures,
        ticksPerSecond = config.ticksPerSecond,
        maxDt = 3,
        cumulativeJoules = cumulative or 0,
    })
end

local function runCycle(bus, aggregator, now, dt)
    local found = collectors.classify(bus:list(), specs)
    aggregator:sync(found, now)
    collectors.pollRound(bus, aggregator, specs, 1000)
    return aggregator:compute(now, dt)
end

local function snapshotOf(aggregator, seq)
    return aggregator:snapshot({ seq = seq or 1, uptime = 10, busOk = true })
end

-- Test 1 : un generateur, 400 J/t
do
    local bus = FakeBus.new(1, 400)
    local aggregator = newAggregator()
    runCycle(bus, aggregator, utils.now(), 1)
    local snapshot = snapshotOf(aggregator)
    check("T1 total generateurs = 1", snapshot.generators.total == 1)
    check("T4 400 J/t -> 160 FE/t", snapshot.production.fe_t == 160, snapshot.production.fe_t)
    check("T4 400 J/t -> 3200 FE/s", snapshot.production.fe_s == 3200, snapshot.production.fe_s)
end

-- Test 2 : 10 generateurs
do
    local bus = FakeBus.new(10, 400)
    local aggregator = newAggregator()
    runCycle(bus, aggregator, utils.now(), 1)
    local snapshot = snapshotOf(aggregator)
    check("T5 10 x 400 J/t = 4000 J/t", snapshot.production.j_t == 4000, snapshot.production.j_t)
    check("T5 1600 FE/t", snapshot.production.fe_t == 1600, snapshot.production.fe_t)
    check("T5 32000 FE/s", snapshot.production.fe_s == 32000, snapshot.production.fe_s)
    check("T2 10 generateurs actifs", snapshot.generators.active == 10 and snapshot.generators.total == 10)
end

-- Test 3 : 100, 500 generateurs, productions differentes
for _, count in ipairs({ 100, 500 }) do
    local bus = FakeBus.new(count, 400)
    local expected = 0
    for index, generator in pairs(bus.generators) do
        generator.jPerTick = 300 + (index % 7) * 25
        expected = expected + generator.jPerTick
    end
    local aggregator = newAggregator()
    runCycle(bus, aggregator, utils.now(), 1)
    local snapshot = snapshotOf(aggregator)
    check(string.format("T3 %d generateurs: somme exacte", count), snapshot.production.j_t == expected,
        snapshot.production.j_t .. " vs " .. expected)
    check(string.format("T3 %d generateurs: total/actifs", count),
        snapshot.generators.total == count and snapshot.generators.active == count)
    local encoded, err = protocol.encode(snapshot)
    check(string.format("T3 %d generateurs: snapshot encodable (<%d octets)", count, protocol.MAX_SIZE),
        encoded ~= nil, err)
end

-- Test 6 : suppression d'un generateur pendant le fonctionnement
do
    local bus = FakeBus.new(100, 400)
    local aggregator = newAggregator()
    local now = utils.now()
    runCycle(bus, aggregator, now, 1)
    bus:remove(42)
    runCycle(bus, aggregator, now + 1, 1)
    local snapshot = snapshotOf(aggregator)
    check("T6 100 -> 99 generateurs", snapshot.generators.total == 99, snapshot.generators.total)
    check("T6 production 99 x 400 J/t", snapshot.production.j_t == 39600, snapshot.production.j_t)
end

-- Test generateur inactif
do
    local bus = FakeBus.new(3, 400)
    local aggregator = newAggregator()
    local now = utils.now()
    runCycle(bus, aggregator, now, 1)
    bus:setActive(2, false)
    runCycle(bus, aggregator, now + 1, 1)
    local snapshot = snapshotOf(aggregator)
    check("Inactif: production = somme des actifs", snapshot.production.j_t == 800, snapshot.production.j_t)
    check("Inactif: 2 actifs / 3", snapshot.generators.active == 2 and snapshot.generators.total == 3)
    bus:setActive(1, false)
    bus:setActive(3, false)
    runCycle(bus, aggregator, now + 2, 1)
    snapshot = snapshotOf(aggregator)
    check("Inactif: 0 FE/t", snapshot.production.fe_t == 0 and snapshot.generators.active == 0)
end

-- Perte de lecture : donnees perimees
do
    local bus = FakeBus.new(2, 400)
    local aggregator = newAggregator()
    local now = utils.now()
    runCycle(bus, aggregator, now, 1)
    aggregator:compute(now + config.staleAfter + 1, 1)
    local snapshot = snapshotOf(aggregator)
    check("Donnees perimees: generateurs invalides, production 0",
        snapshot.generators.invalid == 2 and snapshot.production.j_t == 0 and snapshot.generators.stale == 2)
end

-- Echecs de lecture successifs
do
    local bus = FakeBus.new(2, 400)
    local aggregator = newAggregator()
    local now = utils.now()
    runCycle(bus, aggregator, now, 1)
    local original = bus.invoke
    bus.invoke = function()
        error("no reply from the host: timeout", 0)
    end
    for step = 1, config.maxConsecutiveFailures do
        collectors.pollRound(bus, aggregator, specs, 1000)
        aggregator:compute(now + step, 1)
    end
    local snapshot = snapshotOf(aggregator)
    check("Erreurs de lecture: generateurs invalides sans plantage",
        snapshot.generators.invalid == 2 and snapshot.production.j_t == 0)
    bus.invoke = original
    collectors.pollRound(bus, aggregator, specs, 1000)
    aggregator:compute(now + 10, 1)
    snapshot = snapshotOf(aggregator)
    check("Retour des generateurs apres erreur", snapshot.generators.active == 2 and snapshot.production.j_t == 800)
end

-- Meme generateur vu deux fois
do
    local bus = FakeBus.new(2, 400)
    bus.generators[2].x = bus.generators[1].x
    bus.generators[2].y = bus.generators[1].y
    bus.generators[2].z = bus.generators[1].z
    local aggregator = newAggregator()
    runCycle(bus, aggregator, utils.now(), 1)
    local snapshot = snapshotOf(aggregator)
    check("Doublon de position compte une seule fois",
        snapshot.generators.total == 1 and snapshot.production.j_t == 400, snapshot.generators.total)
end

-- Production cumulee : 250 000 FE/s pendant 10 s = 2 500 000 FE
do
    local bus = FakeBus.new(1, 31250)
    local aggregator = newAggregator()
    local now = utils.now()
    for step = 0, 9 do
        runCycle(bus, aggregator, now + step, 1)
    end
    local snapshot = snapshotOf(aggregator)
    check("Cumul: 250000 FE/s", snapshot.production.fe_s == 250000, snapshot.production.fe_s)
    check("Cumul: 2 500 000 FE apres 10 s", snapshot.cumulative.fe == 2500000, snapshot.cumulative.fe)
    check("Moyenne 1 min = 250000 FE/s", snapshot.stats.avg_1m_fe_s == 250000, snapshot.stats.avg_1m_fe_s)
    check("Produit 1 min = 2 500 000 FE", snapshot.stats.produced_1m_fe == 2500000, snapshot.stats.produced_1m_fe)
end

-- Plafonnement de dt
do
    local bus = FakeBus.new(1, 31250)
    local aggregator = newAggregator()
    aggregator.staleAfter = 1e9
    local now = utils.now()
    runCycle(bus, aggregator, now, 1)
    runCycle(bus, aggregator, now + 1000, 1000)
    local snapshot = snapshotOf(aggregator)
    check("dt plafonne a 3 s", snapshot.cumulative.fe == 250000 * 4, snapshot.cumulative.fe)
end

-- Test 7 : persistance
do
    local persistence = Persistence.new({ dir = tmpDir, log = log, fileName = "production.dat", saveInterval = 30 })
    check("Persistance: sauvegarde", persistence:save(12540000000 * 2.5) == true)
    local reloaded = Persistence.new({ dir = tmpDir, log = log, fileName = "production.dat", saveInterval = 30 })
    check("Persistance: 12.54 GFE restaure", reloaded:load() == 12540000000 * 2.5)
    persistence:save(99.5)
    local handle = io.open(tmpDir .. "/production.dat", "w")
    handle:write("version=1\ncumulative_j=1\n")
    handle:close()
    local recovered = Persistence.new({ dir = tmpDir, log = log, fileName = "production.dat", saveInterval = 30 })
    check("Persistance: fichier tronque -> repli sur la meilleure sauvegarde valide",
        recovered:load() == 12540000000 * 2.5)
    local empty = Persistence.new({ dir = tmpDir .. "/vide", log = log, fileName = "production.dat", saveInterval = 30 })
    check("Persistance: absence de fichier -> 0", empty:load() == 0)
    local aggregator = newAggregator(12540000000 * 2.5)
    local bus = FakeBus.new(0, 400)
    runCycle(bus, aggregator, utils.now(), 1)
    check("Persistance: valeur injectee dans l'agregateur", snapshotOf(aggregator).cumulative.fe == 12540000000)
end

-- Test 11 : unites
do
    local cases = {
        { 0, "/s", "0 FE/s" },
        { 950, "/s", "950 FE/s" },
        { 15400, "/s", "15.40 kFE/s" },
        { 2.8e6, "/s", "2.80 MFE/s" },
        { 4.2e9, "/s", "4.20 GFE/s" },
        { 1.1e12, "/s", "1.10 TFE/s" },
        { 999999, "", "1.00 MFE" },
        { 999.6, "", "1.00 kFE" },
        { 1.5e15, "", "1500.00 TFE" },
        { 842.73e9, "", "842.73 GFE" },
    }
    for _, case in ipairs(cases) do
        local result = utils.formatEnergy(case[1], case[2])
        check(string.format("Unite %s -> %s", tostring(case[1]), case[3]), result == case[3], result)
    end
end

-- Protocole
do
    local bus = FakeBus.new(5, 400)
    local aggregator = newAggregator()
    runCycle(bus, aggregator, utils.now(), 1)
    local snapshot = snapshotOf(aggregator)
    local encoded = protocol.encode(snapshot)
    local decoded, err = protocol.decode(encoded)
    check("Protocole: aller-retour", decoded ~= nil and decoded.production.fe_t == 800, err)
    check("Protocole: snapshot valide", select(1, protocol.validateSnapshot(decoded)) == true)
    check("Protocole: version incompatible refusee",
        select(2, protocol.decode('{"proto":99,"type":"snapshot"}')) == "unsupported_version")
    check("Protocole: JSON invalide refuse", select(1, protocol.decode("{pas du json")) == nil)
    local incomplete = protocol.decode('{"proto":1,"type":"snapshot","production":{}}')
    check("Protocole: snapshot incomplet refuse", select(1, protocol.validateSnapshot(incomplete)) == false)
    check("Protocole: sources desactivees", decoded.sources.solar.enabled == false and decoded.sources.nuclear.enabled == false
        and decoded.sources.wind.enabled == true)
end

os.execute("rm -rf '" .. tmpDir .. "'")
print(string.format("\n%d reussis, %d echecs", passed, failed))
os.exit(failed == 0 and 0 or 1)
]] },
    { name = "S99z_energy_central", content = [[
#!/bin/sh

APP_DIR="/root/energy_monitor/central"
DAEMON="/usr/bin/lua"
PID_FILE="/run/energy_central.pid"

setup_lua_path() {
    if [ -r /etc/profile.d/lua_path.sh ]; then
        . /etc/profile.d/lua_path.sh
    fi
    if [ -z "$LUA_PATH" ]; then
        for dir in /run/oc2/lib/lua /mnt/*/lib/lua; do
            if [ -d "$dir" ]; then
                LUA_PATH="$dir/?.lua;$dir/?/init.lua;;"
                export LUA_PATH
                break
            fi
        done
    fi
}

start() {
    [ -f "$APP_DIR/main.lua" ] || return 0
    printf 'Starting energy central: '
    setup_lua_path
    start-stop-daemon -b -m -S -q -p "$PID_FILE" -x "$DAEMON" -- "$APP_DIR/main.lua"
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "OK"
    else
        echo "FAIL"
    fi
    return "$status"
}

stop() {
    printf 'Stopping energy central: '
    start-stop-daemon -K -q -p "$PID_FILE"
    status=$?
    if [ "$status" -eq 0 ]; then
        rm -f "$PID_FILE"
        echo "OK"
    else
        echo "FAIL"
    fi
    return "$status"
}

restart() {
    stop
    sleep 2
    start
}

case "$1" in
    start|stop|restart)
        "$1";;
    reload)
        restart;;
    *)
        echo "Usage: $0 {start|stop|restart|reload}"
        exit 1
esac
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
