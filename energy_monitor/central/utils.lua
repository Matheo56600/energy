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
