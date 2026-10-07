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
