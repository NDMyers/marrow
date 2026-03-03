class Logger {
    log(message: string): void {
        console.log(message);
    }
}

function formatDate(date: Date): string {
    return date.toISOString();
}
