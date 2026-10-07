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
