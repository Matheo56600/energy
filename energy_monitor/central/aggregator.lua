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
