return {
    centralAddress = "10.0.0.1",
    centralPort = 4710,
    clientId = "screen-1",

    subscribeInterval = 5.0,
    retryInterval = 2.0,
    offlineAfter = 5.0,
    renderInterval = 0.5,
    sizeProbeInterval = 5.0,

    fallbackColumns = 80,
    fallbackRows = 30,
    maxWidth = 72,
    useColor = true,

    energy = {
        feNumerator = 2,
        feDenominator = 5,
    },

    log = {
        fileName = "screen.log",
        maxBytes = 32768,
        repeatInterval = 60,
    },
}
