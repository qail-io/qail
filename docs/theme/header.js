// Inject Astro-like QAIL navigation into mdBook pages
document.addEventListener('DOMContentLoaded', function () {
    if (document.querySelector('.nav')) {
        return;
    }

    const currentPath = window.location.pathname;

    const navLinks = [
        { href: '/', label: 'Home' },
        { href: '/playground', label: 'Playground' },
        { href: '/expressions', label: 'Expressions' },
        { href: '/docs', label: 'Docs' },
        { href: '/drivers', label: 'Drivers' },
        { href: '/blog', label: 'Blog' },
        { href: '/status', label: 'Status' },
        { href: '/roadmap', label: 'Roadmap' },
        { href: '/changelog', label: 'Changelog' }
    ];

    function isActive(href) {
        if (href === '/') {
            return currentPath === '/' || currentPath === '/index.html';
        }
        if (href === '/docs') {
            return currentPath === '/docs' || currentPath === '/docs/' || currentPath.startsWith('/docs/');
        }
        return currentPath === href || currentPath.startsWith(href + '/');
    }

    const nav = document.createElement('nav');
    nav.className = 'nav';

    const linksHtml = navLinks
        .map((link) => `<a href="${link.href}" class="${isActive(link.href) ? 'active' : ''}">${link.label}</a>`)
        .join('');

    const mobileLinksHtml = navLinks
        .map((link) => `<a href="${link.href}" class="${isActive(link.href) ? 'active' : ''}">${link.label}</a>`)
        .join('');

    nav.innerHTML = `
        <div class="nav-container">
            <a href="/" class="nav-logo">
                <span class="logo-icon">🪝</span>
                <span class="logo-text">QAIL</span>
            </a>
            <div class="nav-links">
                ${linksHtml}
            </div>
            <div class="nav-actions">
                <a href="https://github.com/qail-io/qail" target="_blank" rel="noopener" class="btn btn-github btn-icon-only-mobile">
                    <svg height="20" width="20" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
                        <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path>
                    </svg>
                    <span class="btn-text">GitHub</span>
                </a>
                <button class="mobile-toggle" type="button" aria-label="Toggle menu">
                    <span></span>
                    <span></span>
                    <span></span>
                </button>
            </div>
        </div>
    `;

    const mobileMenu = document.createElement('div');
    mobileMenu.className = 'mobile-menu';
    mobileMenu.id = 'mobileMenu';
    mobileMenu.innerHTML = `
        ${mobileLinksHtml}
        <a href="https://github.com/qail-io/qail" target="_blank" rel="noopener">GitHub</a>
    `;

    document.body.insertBefore(nav, document.body.firstChild);
    document.body.insertBefore(mobileMenu, nav.nextSibling);

    const toggleButton = nav.querySelector('.mobile-toggle');
    if (toggleButton) {
        toggleButton.addEventListener('click', function () {
            mobileMenu.classList.toggle('active');
        });
    }

    mobileMenu.querySelectorAll('a').forEach((link) => {
        link.addEventListener('click', () => mobileMenu.classList.remove('active'));
    });

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') {
            mobileMenu.classList.remove('active');
        }
    });
});
