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
