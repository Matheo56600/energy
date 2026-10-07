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
