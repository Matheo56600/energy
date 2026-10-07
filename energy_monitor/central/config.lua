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
