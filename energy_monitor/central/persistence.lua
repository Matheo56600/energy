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
