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
