// Inject QAIL header into mdBook pages
// Wait for DOM to be ready
document.addEventListener('DOMContentLoaded', function () {
    // Check if header already exists to prevent duplicates
    if (document.querySelector('.qail-header')) {
        return;
    }

    const header = document.createElement('nav');
    header.className = 'qail-header';
    header.innerHTML = `
        <a href="/" class="qail-logo">
            <span class="qail-icon">🪝</span>
            <span class="qail-text">QAIL</span>
        </a>
        <div class="qail-nav-links">
            <a href="/">Home</a>
            <a href="/playground">Playground</a>
            <a href="/docs" class="active">Docs</a>
            <a href="/benchmarks">Benchmarks</a>
            <a href="https://github.com/qail-io/qail" target="_blank">GitHub</a>
        </div>
    `;
    document.body.insertBefore(header, document.body.firstChild);
});
