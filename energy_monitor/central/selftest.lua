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
