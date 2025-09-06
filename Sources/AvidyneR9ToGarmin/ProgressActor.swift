import Progress

actor ProgressActor {
    private var progressBar: ProgressBar?
    private var lastProgressPercentage = 0
    private var isInitialized = false
    
    init(verbose: Bool) {
        if !verbose {
            self.progressBar = ProgressBar(count: 100, configuration: [
                ProgressString(string: "Processing: "),
                ProgressBarLine(),
                ProgressPercent()
            ])
        }
    }
    
    func update(progress: Float, message: String) {
        // Ensure we don't go backwards
        let percentComplete = min(100, max(0, Int(progress * 100)))
        
        if !isInitialized {
            isInitialized = true
            // Make sure the progress bar shows immediately
            print("\u{1B}[?25l") // Hide cursor
        }
        
        let delta = percentComplete - lastProgressPercentage
        if delta > 0 {
            for _ in 0..<delta {
                progressBar?.next()
            }
            lastProgressPercentage = percentComplete
        }
    }
    
    func finish() {
        while lastProgressPercentage < 100 {
            progressBar?.next()
            lastProgressPercentage += 1
        }
        print("\u{1B}[?25h") // Show cursor
        print("")  // New line after progress bar
    }
}
