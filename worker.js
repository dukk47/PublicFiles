#!/usr/bin/env node

/**
 * IMGFlow Analyze Worker
 *
 * A queue-based worker that processes website analysis jobs using Puppeteer.
 * - Keeps browser instance open for efficiency
 * - Polls database for pending jobs
 * - Parallel page processing for faster analysis
 * - Designed to run on separate servers for scaling
 *
 * Usage: node scripts/analyze-worker.js [options]
 *   --worker-id NAME    Set custom worker ID (default: random)
 *   --parallel N        Number of pages to process in parallel (default: auto-detect CPU cores)
 *   --help              Show this help
 */

const puppeteer = require('puppeteer');
const { PrismaClient } = require('@prisma/client');
const os = require('os');
const fs = require('fs');
const path = require('path');

// ============================================================================
// LOCAL STORAGE: Store results locally, only send to server at completion
// This reduces database load and prevents connection issues during analysis
// ============================================================================
const LOCAL_RESULTS_DIR = path.join(os.tmpdir(), 'imgflow-worker-results');

// Ensure local results directory exists
function ensureLocalResultsDir() {
  if (!fs.existsSync(LOCAL_RESULTS_DIR)) {
    fs.mkdirSync(LOCAL_RESULTS_DIR, { recursive: true });
  }
}

// Get local file path for a job
function getLocalResultPath(jobId) {
  return path.join(LOCAL_RESULTS_DIR, `${jobId}.json`);
}

// Save result to local file (atomic write)
function saveLocalResult(jobId, result) {
  ensureLocalResultsDir();
  const filePath = getLocalResultPath(jobId);
  const tempPath = filePath + '.tmp';
  fs.writeFileSync(tempPath, JSON.stringify(result, null, 2));
  fs.renameSync(tempPath, filePath); // Atomic rename
}

// Load result from local file
function loadLocalResult(jobId) {
  const filePath = getLocalResultPath(jobId);
  if (fs.existsSync(filePath)) {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }
  return null;
}

// Delete local result file
function deleteLocalResult(jobId) {
  const filePath = getLocalResultPath(jobId);
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}

let prisma = new PrismaClient();

// Retry wrapper for database operations (handles connection timeouts)
async function withRetry(operation, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      const isConnectionError = error.message.includes('closed the connection') ||
        error.message.includes('Connection refused') ||
        error.message.includes('ECONNRESET') ||
        error.code === 'P1017' || // Prisma: Server has closed the connection
        error.code === 'P1001';   // Prisma: Can't reach database server

      if (isConnectionError && attempt < maxRetries) {
        log(`DB connection lost, reconnecting (attempt ${attempt}/${maxRetries})...`, 'warn');
        await prisma.$disconnect();
        prisma = new PrismaClient();
        await new Promise(r => setTimeout(r, 1000 * attempt)); // Exponential backoff
      } else {
        throw error;
      }
    }
  }
}

// Check if a URL is an image file (not a real page)
// Matches extension at end or before query string
const IMAGE_EXTENSIONS = /\.(jpg|jpeg|png|gif|webp|svg|ico|bmp|avif)($|\?|#)/i;
function isImageUrl(path) {
  return IMAGE_EXTENSIONS.test(path);
}

// Check if a URL is a downloadable file (not a page to crawl)
// Includes: archives, documents, media, fonts, data files, calendar, contacts, config files, design files
// Matches extension at end or before query string
const FILE_EXTENSIONS = /\.(pdf|zip|rar|7z|tar|gz|bz2|xz|doc|docx|xls|xlsx|ppt|pptx|odt|ods|odp|mp3|mp4|avi|mov|wmv|flv|wav|ogg|m4a|aac|webm|mkv|exe|dmg|iso|apk|deb|rpm|msi|css|js|json|xml|txt|csv|sql|woff|woff2|ttf|eot|otf|ics|vcs|ical|vcf|vcard|yml|yaml|toml|ini|conf|cfg|log|bak|tmp|swp|rss|atom|gpx|kml|kmz|epub|mobi|azw|eps|ai|psd|indd|sketch|xd|tif|tiff|raw|cr2|nef|dng)($|\?|#)/i;
function isFileUrl(path) {
  return FILE_EXTENSIONS.test(path) || IMAGE_EXTENSIONS.test(path);
}

// Check if URL is a cart/AJAX action (not a real page)
// Only filter WooCommerce cart actions, not general ?action= params
const CART_ACTION_PATTERNS = /(\?|&)(add-to-cart|add_to_cart|remove-item|remove_item|wc-ajax|add-to-wishlist)=/i;
function isActionUrl(path) {
  return CART_ACTION_PATTERNS.test(path);
}

// Normalize image URL to get base image (groups all size variants)
// Extracts filename and removes:
// - WordPress size suffix: -600x450
// - Elementor hash: -abc123def456... (20+ chars)
function normalizeImageUrl(src) {
  try {
    // Get filename from URL
    const url = new URL(src, 'http://dummy');
    let filename = url.pathname.split('/').pop();

    // Remove Elementor hash (20+ alphanumeric chars before extension)
    filename = filename.replace(/-[a-z0-9]{20,}(\.[a-zA-Z]+)$/i, '$1');

    // Remove WordPress/Elementor size suffix: -WIDTHxHEIGHT or -WIDTHxHEIGHT-N
    filename = filename.replace(/-\d+x\d+(-\d+)?(\.[a-zA-Z]+)$/, '$2');

    return filename.toLowerCase();
  } catch {
    // Fallback: just use the src as-is
    return src;
  }
}

// Screenshot config (can be overridden via --screenshot-every N)
let SCREENSHOT_EVERY_N_PAGES = 50; // Take screenshot every N pages (default)
const SCREENSHOT_WIDTH = 800; // Display width for screenshots
const SCREENSHOT_HEIGHT = 600; // Proportional height
const ANIMATION_WAIT = 6000; // Wait 6 seconds for fade-in animations before screenshot
const IMGFLOW_SERVER = process.env.IMGFLOW_SERVER || 'https://imgflow.stempel-it.de';

// Auto-retry config
const MAX_JOB_RETRIES = 3; // Jobs are abandoned after this many failures

// Handle job failure with auto-retry logic
// Returns true if job was retried, false if abandoned
async function handleJobFailure(job, error, domainStatus = null) {
  const currentRetries = job.retryCount || 0;

  // Check if job has a checkpoint (result with crawlState or pages)
  const hasCheckpoint = job.result && typeof job.result === 'object' &&
    ('crawlState' in job.result || (job.result.pages && job.result.pages.length > 0));

  if (currentRetries < MAX_JOB_RETRIES) {
    // Retry: preserve checkpoint if exists (set to 'processing' to resume)
    await withRetry(() => prisma.analyzeJob.update({
      where: { id: job.id },
      data: {
        status: hasCheckpoint ? 'processing' : 'pending', // Resume if checkpoint exists
        retryCount: currentRetries + 1,
        error: `Retry ${currentRetries + 1}/${MAX_JOB_RETRIES}: ${error}`,
        workerId: null,
        // Keep startedAt if resuming, reset if starting fresh
        startedAt: hasCheckpoint ? job.startedAt : null,
        completedAt: null,
        // NEVER wipe result - preserve checkpoint for resume!
      },
    }));
    const pagesProcessed = hasCheckpoint ? (job.result.pages?.length || 0) : 0;
    log(`Job ${job.id.substring(0, 8)} failed, queued for retry (${currentRetries + 1}/${MAX_JOB_RETRIES})${hasCheckpoint ? ` [${pagesProcessed} pages preserved]` : ''}: ${error}`, 'warn');
    return true;
  } else {
    // Max retries reached: mark as abandoned
    await withRetry(() => prisma.analyzeJob.update({
      where: { id: job.id },
      data: {
        status: 'abandoned',
        completedAt: new Date(),
        domainStatus: domainStatus,
        error: `Abandoned after ${MAX_JOB_RETRIES} attempts: ${error}`,
        workerId: null,
      },
    }));
    log(`Job ${job.id.substring(0, 8)} abandoned after ${MAX_JOB_RETRIES} failures: ${error}`, 'error');
    return false;
  }
}

// Upload screenshot to IMGFlow server
async function uploadScreenshot(jobId, filename, screenshotBuffer) {
  const { Readable } = require('stream');

  const FormData = (await import('form-data')).default;
  const fetch = (await import('node-fetch')).default;

  // Convert buffer to readable stream for form-data
  const stream = new Readable();
  stream.push(screenshotBuffer);
  stream.push(null);

  const form = new FormData();
  form.append('screenshot', stream, { filename, contentType: 'image/jpeg', knownLength: screenshotBuffer.length });
  form.append('jobId', jobId);
  form.append('filename', filename);

  const response = await fetch(`${IMGFLOW_SERVER}/api/analyze/screenshot`, {
    method: 'POST',
    body: form,
    headers: form.getHeaders(),
    timeout: 30000, // 30 second timeout
  });

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(`Upload failed: ${response.status} - ${responseText.substring(0, 100)}`);
  }

  // Try to parse JSON
  try {
    const data = JSON.parse(responseText);
    return data.url;
  } catch (e) {
    throw new Error(`Invalid response: ${responseText.substring(0, 100)}`);
  }
}

// Trigger AI categorization for completed job (fire and forget)
async function triggerCategorization(jobId) {
  try {
    const fetch = (await import('node-fetch')).default;
    const response = await fetch(`${IMGFLOW_SERVER}/api/analyze/categorize`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jobId: jobId,
        workerSecret: 'imgflow-worker-2024'
      }),
      timeout: 30000
    });

    if (response.ok) {
      const data = await response.json();
      if (data.alreadyCategorized) {
        log(`  AI: Already categorized as ${data.category}`, 'info');
      } else {
        log(`  AI: Categorized as ${data.category} (${data.industry})`, 'success');
      }
    } else {
      const errorText = await response.text();
      log(`  AI: Categorization failed - ${errorText.substring(0, 100)}`, 'warn');
    }
  } catch (error) {
    log(`  AI: Categorization error - ${error.message}`, 'warn');
  }
}

// Parse command line arguments
const args = process.argv.slice(2);

// Show help
if (args.includes('--help') || args.includes('-h')) {
  console.log(`
IMGFlow Analyze Worker (Round-Robin Mode)

Usage: node analyze-worker.js [options]

Options:
  --worker-id NAME      Set custom worker ID (default: random)
  --parallel N          Number of pages to process in parallel
                        Default: auto-detect based on CPU cores (${os.cpus().length} on this machine)
                        Recommended: 2-4 for low-end VPS, 4-8 for mid-range, 8-16 for high-end
  --screenshot-every N  Take screenshot every N pages (default: 50)
                        Use lower values (5-10) for more frequent screenshots
  --pick                Show pending jobs and pick one to process
  --help                Show this help

Round-Robin Mode:
  Workers process ${roundRobinChunkSize} pages per job, then release it for the next worker.
  This ensures fair scheduling across all jobs in the queue.

  Job Priority:
    1. Released jobs (status=processing, no worker) - ordered by oldest updatedAt
    2. Abandoned jobs (no update in 10 min) - worker crashed recovery
    3. New pending jobs - ordered by createdAt (FIFO)

  Per-Job Page Limits:
    Each job can have its own maxPages setting:
    - null: Use admin default from settings
    - 0: Unlimited pages
    - N: Max N pages

Examples:
  node analyze-worker.js                                    # Queue worker mode (auto)
  node analyze-worker.js --parallel 4                       # Process 4 pages at once
  node analyze-worker.js --pick                             # Pick a job interactively
  node analyze-worker.js --pick --parallel 8                # Pick with 8 threads
`);
  process.exit(0);
}

// Parse worker ID
let WORKER_ID = `${os.hostname().split('.')[0]}-${Math.random().toString(36).substring(7)}`;
const workerIdx = args.indexOf('--worker-id');
if (workerIdx !== -1 && args[workerIdx + 1]) {
  WORKER_ID = args[workerIdx + 1];
}

// Parse parallel pages count
let PARALLEL_PAGES;
const parallelIdx = args.indexOf('--parallel');
if (parallelIdx !== -1) {
  const nextArg = args[parallelIdx + 1];
  // Make sure next arg exists and is not another flag
  if (nextArg && !nextArg.startsWith('-')) {
    const parsed = parseInt(nextArg, 10);
    if (isNaN(parsed) || parsed < 1) {
      console.error('Error: --parallel must be a positive number (got: ' + nextArg + ')');
      process.exit(1);
    }
    PARALLEL_PAGES = Math.max(1, parsed); // Minimum 1, no upper limit
  } else {
    console.error('Error: --parallel requires a number (e.g., --parallel 4)');
    process.exit(1);
  }
} else {
  // Auto-detect based on CPU cores
  // Use half of available cores, minimum 2, maximum 8
  const cpuCount = os.cpus().length;
  PARALLEL_PAGES = Math.min(8, Math.max(2, Math.floor(cpuCount / 2)));
}

// Parse screenshot interval
const screenshotIdx = args.indexOf('--screenshot-every');
if (screenshotIdx !== -1) {
  const nextArg = args[screenshotIdx + 1];
  if (nextArg && !nextArg.startsWith('-')) {
    const parsed = parseInt(nextArg, 10);
    if (isNaN(parsed) || parsed < 1) {
      console.error('Error: --screenshot-every must be a positive number (got: ' + nextArg + ')');
      process.exit(1);
    }
    SCREENSHOT_EVERY_N_PAGES = parsed;
  } else {
    console.error('Error: --screenshot-every requires a number (e.g., --screenshot-every 10)');
    process.exit(1);
  }
}

// Parse --pick for interactive job selection
const PICK_MODE = args.includes('--pick');

// Config
const POLL_INTERVAL = 2000; // Check for new jobs every 2 seconds
const PAGE_TIMEOUT = 60000; // 60 second timeout per page
const IMGFLOW_WAIT = 3000; // Wait for IMGFlow JS to apply alts
let roundRobinChunkSize = 50; // Pages to process per job before moving to next (round-robin)

// Realistic Chrome User-Agent (helps bypass basic bot detection)
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// Adaptive throttling config
const THROTTLE_ERROR_THRESHOLD = 3; // Consecutive errors before throttling
const THROTTLE_RECOVERY_PAGES = 5; // Successful pages before increasing parallelism
const THROTTLE_MIN_PARALLEL = 1; // Minimum parallel pages when throttled
const THROTTLE_DELAY_MS = 2000; // Delay between requests when heavily throttled
const SLOW_MODE_DELAY_MS = 30000; // 30 second delay when in slow mode (many consecutive errors)
const MAX_JOB_TIME_MS = 5 * 60 * 1000; // 5 minutes max per job session in slow mode, then release with checkpoint
const CONNECTION_ERRORS = ['ERR_CONNECTION_REFUSED', 'ERR_CONNECTION_RESET', 'ERR_CONNECTION_TIMED_OUT', 'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'net::ERR_'];

// Dynamic settings from admin panel (fetched on startup and periodically)
let analyzeMaxPages = 500; // Default max pages per analysis
let analyzeUnlimitedDomains = []; // Domains without page limit
let analyzePagesPerHour = 100; // Default rate limit: max pages per hour per job
let bgMaxPages = 200; // Default max pages for background jobs
let bgMaxJobs = 100; // Default max active background jobs

// Fetch settings from admin API
async function fetchSettings() {
  try {
    const response = await fetch(`${IMGFLOW_SERVER}/api/analyze/settings`);
    if (response.ok) {
      const data = await response.json();
      analyzeMaxPages = data.maxPages !== undefined ? data.maxPages : 500;
      analyzePagesPerHour = data.pagesPerHour !== undefined ? data.pagesPerHour : 100;
      analyzeUnlimitedDomains = (data.unlimitedDomains || '')
        .split(',')
        .map(d => d.trim().toLowerCase())
        .filter(d => d.length > 0);
      bgMaxPages = data.bgMaxPages !== undefined ? data.bgMaxPages : 200;
      bgMaxJobs = data.bgMaxJobs !== undefined ? data.bgMaxJobs : 100;
      roundRobinChunkSize = data.roundRobinChunkSize !== undefined ? data.roundRobinChunkSize : 50;
      log(`Settings loaded: maxPages=${analyzeMaxPages}, pagesPerHour=${analyzePagesPerHour}, bgMaxPages=${bgMaxPages}, bgMaxJobs=${bgMaxJobs}, roundRobin=${roundRobinChunkSize}`, 'info');
    }
  } catch (e) {
    log(`Could not fetch settings: ${e.message}`, 'warn');
  }
}

// Check if a domain should have unlimited pages
function isUnlimitedDomain(hostname) {
  const normalizedHost = hostname.toLowerCase();
  return analyzeUnlimitedDomains.some(d =>
    normalizedHost === d || normalizedHost.endsWith('.' + d)
  );
}

// Check if a job is rate-limited (too many pages in the last hour)
// Returns { limited: boolean, pagesInLastHour: number, limit: number, waitMinutes: number }
function checkJobRateLimit(job) {
  // Get rate limit: per-job setting > admin setting (0 = unlimited)
  const limit = job.pagesPerHour !== null && job.pagesPerHour !== undefined
    ? job.pagesPerHour
    : analyzePagesPerHour;

  // 0 or null means unlimited
  if (!limit || limit === 0) {
    return { limited: false, pagesInLastHour: 0, limit: 0, waitMinutes: 0 };
  }

  // Get timestamps from crawlState
  const timestamps = job.result?.crawlState?.processedTimestamps || [];
  const oneHourAgo = Date.now() - (60 * 60 * 1000);

  // Count pages processed in the last hour
  const pagesInLastHour = timestamps.filter(ts => ts > oneHourAgo).length;

  if (pagesInLastHour >= limit) {
    // Find oldest timestamp and calculate wait time
    const oldestInWindow = timestamps.filter(ts => ts > oneHourAgo).sort((a, b) => a - b)[0];
    const waitMs = oldestInWindow ? (oldestInWindow + 60 * 60 * 1000 - Date.now()) : 0;
    const waitMinutes = Math.ceil(waitMs / 60000);

    return { limited: true, pagesInLastHour, limit, waitMinutes };
  }

  return { limited: false, pagesInLastHour, limit, waitMinutes: 0 };
}

// Email sending via API (no nodemailer needed on worker machine)
async function sendNotificationEmail(job, result) {
  if (!job.notifyEmail || job.emailSent) return;

  // Calculate stats from result.pages (same logic as Results page)
  // Use the global isImageUrl function

  const normalizeImageUrl = (src) => {
    try {
      const url = new URL(src, 'http://dummy');
      let filename = url.pathname.split('/').pop() || src;
      filename = filename.replace(/-[a-z0-9]{20,}(\.[a-zA-Z]+)$/i, '$1');
      filename = filename.replace(/-\d+x\d+(-\d+)?(\.[a-zA-Z]+)$/, '$2');
      return filename.toLowerCase();
    } catch { return src; }
  };

  const pages = result?.pages || [];
  const realPages = pages.filter(page => !isImageUrl(page.path || page.url));

  const imageStats = new Map();
  let totalOccurrences = 0;
  let occurrencesWithAlt = 0;

  for (const page of realPages) {
    if (!page.images) continue;
    for (const img of page.images) {
      const name = normalizeImageUrl(img.src);
      if (!imageStats.has(name)) {
        imageStats.set(name, { total: 0, withAlt: 0 });
      }
      const stats = imageStats.get(name);
      stats.total++;
      totalOccurrences++;
      if (img.hasAlt) {
        stats.withAlt++;
        occurrencesWithAlt++;
      }
    }
  }

  const uniqueImages = imageStats.size;
  const score = totalOccurrences > 0 ? Math.round((occurrencesWithAlt / totalOccurrences) * 100) : 100;
  const missingAlt = totalOccurrences - occurrencesWithAlt;
  const hostname = new URL(job.url).hostname;

  try {
    // Call API to send email (works without nodemailer on worker machine)
    const response = await fetch('https://imgflow.de/api/send-notification', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer imgflow-notify-2024',
      },
      body: JSON.stringify({
        jobId: job.id,
        email: job.notifyEmail,
        hostname: hostname,
        stats: {
          pagesCount: realPages.length,
          uniqueImages,
          occurrencesWithAlt,
          totalOccurrences,
          score,
          missingAlt,
        },
      }),
    });

    if (!response.ok) {
      throw new Error(`API responded with ${response.status}`);
    }

    // Mark email as sent
    await prisma.analyzeJob.update({
      where: { id: job.id },
      data: { emailSent: true },
    });

    log(`Email sent to ${job.notifyEmail}`, 'success');
  } catch (error) {
    log(`Failed to send email: ${error.message}`, 'error');
  }
}

// Colors for console
const c = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  gray: '\x1b[90m',
  red: '\x1b[31m',
};

function log(msg, type = 'info') {
  const timestamp = new Date().toISOString().substring(11, 19);
  const prefix = {
    info: `${c.gray}[${timestamp}]${c.reset} ${c.cyan}[${WORKER_ID}]${c.reset}`,
    success: `${c.gray}[${timestamp}]${c.reset} ${c.green}[${WORKER_ID}]${c.reset}`,
    error: `${c.gray}[${timestamp}]${c.reset} ${c.red}[${WORKER_ID}]${c.reset}`,
    warn: `${c.gray}[${timestamp}]${c.reset} ${c.yellow}[${WORKER_ID}]${c.reset}`,
  };
  console.log(`${prefix[type]} ${msg}`);
}

// Analyze a single page and return result
async function analyzeSinglePage(page, fullUrl, urlObj, basicAuth) {
  // Set up auth if provided
  if (basicAuth) {
    const [username, password] = basicAuth.split(':');
    await page.authenticate({ username, password });
  }

  // Use domcontentloaded first, then race between networkidle2 and a 20s timeout
  // This handles sites with chat widgets/analytics that never go truly idle
  const response = await page.goto(fullUrl, {
    waitUntil: 'domcontentloaded',
    timeout: PAGE_TIMEOUT,
  });

  // Wait for network to be idle OR max 20 seconds (whichever comes first)
  await Promise.race([
    page.waitForNetworkIdle({ idleTime: 500, timeout: 20000 }).catch(() => {}),
    new Promise(r => setTimeout(r, 20000))
  ]);

  if (!response || response.status() >= 400) {
    return { images: [], links: [], error: `HTTP ${response?.status() || 'unknown'}` };
  }

  // Check if we were redirected to an external domain
  const originalHostname = urlObj.hostname.replace(/^www\./, '');
  const finalUrl = page.url();
  try {
    const finalHostname = new URL(finalUrl).hostname.replace(/^www\./, '');
    if (finalHostname !== originalHostname &&
        !finalHostname.endsWith('.' + originalHostname) &&
        !originalHostname.endsWith('.' + finalHostname)) {
      return { images: [], links: [], error: `Redirected to external domain: ${finalHostname}`, skipped: true };
    }
  } catch (e) {
    // URL parsing failed, continue anyway
  }

  // STEP 1: Get images from initial HTML response (these would be OB candidates)
  // Fetch raw HTML without JS execution
  let htmlImageSrcs = new Set();
  try {
    const headers = { 'User-Agent': USER_AGENT };
    if (basicAuth) {
      headers['Authorization'] = 'Basic ' + Buffer.from(basicAuth).toString('base64');
    }
    const rawHtmlResponse = await fetch(fullUrl, { headers });
    const rawHtml = await rawHtmlResponse.text();

    // Extract image srcs from raw HTML using regex
    const imgTagRegex = /<img[^>]+src=["']([^"']+)["'][^>]*>/gi;
    let match;
    while ((match = imgTagRegex.exec(rawHtml)) !== null) {
      let src = match[1];
      if (src && !src.startsWith('data:')) {
        // Normalize relative URLs
        if (src.startsWith('//')) {
          src = 'https:' + src;
        } else if (src.startsWith('/')) {
          src = urlObj.origin + src;
        } else if (!src.startsWith('http')) {
          src = new URL(src, fullUrl).href;
        }
        htmlImageSrcs.add(src);
      }
    }
  } catch (e) {
    // Fall back to treating all images as JS-loaded
  }

  // STEP 2: Wait for JS to load all dynamic images
  await new Promise(r => setTimeout(r, IMGFLOW_WAIT));

  // Scroll to trigger lazy loading
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await new Promise(r => setTimeout(r, 1500));
  await page.evaluate(() => window.scrollTo(0, 0));

  // STEP 3: Get all images after JS execution
  const pageData = await page.evaluate((htmlSrcs) => {
    const htmlSrcSet = new Set(htmlSrcs);
    const images = [];

    document.querySelectorAll('img').forEach(img => {
      // Skip tiny images
      if (img.width < 10 || img.height < 10) return;
      if (!img.src || img.src.startsWith('data:')) return;

      const currentAlt = img.alt || '';
      const hasAltAttr = img.hasAttribute('alt');
      const hasAlt = hasAltAttr && currentAlt.trim().length > 0;

      // Check if image was in initial HTML (OB) or loaded by JS
      const wouldBeOB = htmlSrcSet.has(img.src);

      images.push({
        src: img.src,
        alt: currentAlt,
        hasAlt: hasAlt,
        emptyAlt: hasAltAttr && currentAlt.trim() === '',
        imgflowMethod: wouldBeOB ? 'ob' : 'js',
        width: img.naturalWidth || img.width,
        height: img.naturalHeight || img.height,
      });
    });
    return images;
  }, [...htmlImageSrcs]);

  // Extract links for crawling (only internal links)
  const hostname = urlObj.hostname;
  const links = await page.evaluate((hostname) => {
    const anchors = document.querySelectorAll('a[href]');
    const internalLinks = [];

    anchors.forEach(a => {
      let href = a.getAttribute('href');
      if (!href) return;

      // Handle protocol-relative URLs (//www.example.com/path)
      if (href.startsWith('//')) {
        if (!href.includes(hostname)) return; // External
        href = 'https:' + href; // Convert to full URL for parsing
      }

      if (href.startsWith('http') && !href.includes(hostname)) return;
      if (href.startsWith('#') || href.startsWith('javascript:') ||
          href.startsWith('mailto:') || href.startsWith('tel:')) return;

      if (href.startsWith('http')) href = new URL(href).pathname;
      if (!href.startsWith('/')) href = '/' + href;
      if (href !== '/' && href.endsWith('/')) href = href.slice(0, -1);

      internalLinks.push(href);
    });

    return [...new Set(internalLinks)];
  }, hostname);

  return { images: pageData, links };
}

// Check if an error is a connection/rate-limit error
function isConnectionError(errorMessage) {
  return CONNECTION_ERRORS.some(e => errorMessage.includes(e));
}

async function analyzeWebsite(browser, url, basicAuth, jobId, onProgress = null, previousState = null, options = {}) {
  // Options for round-robin processing and rate limiting
  const { chunkSize = Infinity, jobMaxPages = null, jobPagesPerHour = null, isBackground = false } = options;

  // Calculate rate limit for this job
  const rateLimit = jobPagesPerHour !== null && jobPagesPerHour !== undefined
    ? jobPagesPerHour
    : analyzePagesPerHour;
  const hasRateLimit = rateLimit && rateLimit > 0;
  // Track unique images by their normalized filename
  // Map: normalizedFilename -> { hasAlt: boolean, files: number, firstSrc: string }
  const uniqueImagesMap = new Map();

  // Adaptive throttling state
  let currentParallel = PARALLEL_PAGES; // Start with configured parallelism
  let consecutiveErrors = 0; // Track consecutive connection errors
  let successfulPages = 0; // Track successful pages since last throttle
  let isThrottled = false;
  let isSlowMode = false; // Extra slow mode after many consecutive errors
  let consecutiveDetachedErrors = 0; // Track browser corruption errors
  const jobSessionStart = Date.now(); // Track when this session started for time limit

  // Resume from previous state if available (either crawlState OR existing pages)
  const isResuming = previousState && (previousState.crawlState || previousState.pages?.length > 0);
  if (isResuming) {
    const hasCrawlState = !!previousState.crawlState;
    log(`  Resuming from previous state: ${previousState.pages?.length || 0} pages already processed ${hasCrawlState ? '(with checkpoint)' : '(reconstructing from pages)'}`, 'info');
  }

  const result = {
    pages: isResuming ? (previousState.pages || []) : [],
    pagesQueued: 0, // Total URLs discovered (for progress tracking)
    screenshot: isResuming ? (previousState.screenshot || null) : null,
    screenshots: isResuming ? (previousState.screenshots || []) : [],
    summary: isResuming ? (previousState.summary || {
      totalFiles: 0, uniqueImages: 0, totalImages: 0, withAlt: 0, withoutAlt: 0,
      obProcessed: 0, jsProcessed: 0, noAlt: 0, emptyAlt: 0, unchanged: 0,
    }) : {
      totalFiles: 0,      // Total image files (including size variants)
      uniqueImages: 0,    // Unique images (after normalization)
      totalImages: 0,     // DEPRECATED: Use uniqueImages (for backwards compat)
      withAlt: 0,         // Unique images with alt
      withoutAlt: 0,      // Unique images without alt
      obProcessed: 0,
      jsProcessed: 0,
      noAlt: 0,
      emptyAlt: 0,
      unchanged: 0,
    },
    // Crawl state for resume capability
    crawlState: null,
  };

  // Restore uniqueImagesMap from previous pages if resuming
  if (isResuming && previousState.pages) {
    for (const page of previousState.pages) {
      for (const img of (page.images || [])) {
        const normalizedName = normalizeImageUrl(img.src);
        if (!uniqueImagesMap.has(normalizedName)) {
          uniqueImagesMap.set(normalizedName, {
            hasAlt: img.hasAlt,
            files: 1,
            firstSrc: img.src,
          });
        } else {
          const existing = uniqueImagesMap.get(normalizedName);
          existing.files++;
          if (img.hasAlt) existing.hasAlt = true;
        }
      }
    }
  }

  let pageCounter = isResuming ? (previousState.crawlState?.pageCounter || previousState.pages?.length || 0) : 0;

  // Parse URL
  const urlObj = new URL(url);

  // EARLY CHECK: Test if domain is alive before doing full analysis
  // Only do this check on fresh starts, not when resuming
  if (!isResuming) {
    try {
      const testPage = await browser.newPage();
      await testPage.setUserAgent(USER_AGENT);

      // Stealth for early check too
      await testPage.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
      });

      // Block downloads on test page too
      const testClient = await testPage.createCDPSession();
      await testClient.send('Page.setDownloadBehavior', { behavior: 'deny' });

      let domainStatus = 'alive';
      let earlyError = null;

      try {
        const testResponse = await testPage.goto(url, {
          waitUntil: 'domcontentloaded',
          timeout: 15000, // Quick timeout for initial check
        });

        if (!testResponse) {
          domainStatus = 'dead';
          earlyError = 'No response from server';
        } else {
          const status = testResponse.status();
          const serverHeader = (testResponse.headers()['server'] || '').toLowerCase();
          const pageContent = await testPage.content();
          const textContent = pageContent.replace(/<[^>]*>/g, '').trim();

          // Check for blocked/forbidden
          if (status === 403 || status === 401) {
            domainStatus = 'blocked';
            earlyError = `HTTP ${status} - Access denied`;
          }
          // Check for parking servers (HTTP 405 or "Parking" server header)
          else if (status === 405 || serverHeader.includes('parking')) {
            domainStatus = 'parked';
            earlyError = 'Domain is parked';
          }
          // Check for parked domains (common parking page indicators)
          else if (
            pageContent.includes('domain is for sale') ||
            pageContent.includes('domain zum verkauf') ||
            pageContent.includes('buy this domain') ||
            pageContent.includes('This domain may be for sale') ||
            pageContent.includes('parked free') ||
            pageContent.includes('sedoparking') ||
            pageContent.includes('hugedomains') ||
            pageContent.includes('dan.com') ||
            pageContent.includes('afternic')
          ) {
            domainStatus = 'parked';
            earlyError = 'Domain is parked/for sale';
          }
          // Check for dead/empty sites
          else if (status >= 500) {
            domainStatus = 'dead';
            earlyError = `HTTP ${status} - Server error`;
          }
          // Check for nearly empty pages (less than 100 chars of text = probably dead)
          else if (status === 200 && textContent.length < 100) {
            domainStatus = 'dead';
            earlyError = 'Site appears empty or broken';
          }
        }
      } catch (e) {
        const errorMsg = e.message.toLowerCase();
        if (errorMsg.includes('net::err_name_not_resolved') ||
            errorMsg.includes('dns') ||
            errorMsg.includes('getaddrinfo')) {
          domainStatus = 'dead';
          earlyError = 'DNS lookup failed - domain does not exist';
        } else if (errorMsg.includes('timeout')) {
          // Navigation timeout = site is too slow or hanging
          domainStatus = 'unreachable';
          earlyError = 'Site too slow or not responding';
        } else if (errorMsg.includes('net::err_connection_refused') ||
                   errorMsg.includes('net::err_connection_reset') ||
                   errorMsg.includes('net::err_connection_timed_out')) {
          domainStatus = 'unreachable';
          earlyError = 'Connection refused/timeout';
        } else if (errorMsg.includes('net::err_ssl') ||
                   errorMsg.includes('ssl') ||
                   errorMsg.includes('certificate')) {
          domainStatus = 'unreachable';
          earlyError = 'SSL/Certificate error';
        } else if (errorMsg.includes('net::err_invalid_response')) {
          domainStatus = 'dead';
          earlyError = 'Invalid server response';
        } else {
          domainStatus = 'unreachable';
          earlyError = e.message;
        }
      }

      await testPage.close();

      // If domain is not alive, return early with the status
      if (domainStatus !== 'alive') {
        log(`  Domain status: ${domainStatus} - ${earlyError}`, 'warn');
        return {
          pages: [],
          pagesQueued: 0,
          summary: { totalFiles: 0, uniqueImages: 0, totalImages: 0, withAlt: 0, withoutAlt: 0, obProcessed: 0, jsProcessed: 0, noAlt: 0, emptyAlt: 0, unchanged: 0 },
          crawlState: null,
          domainStatus: domainStatus,
          error: earlyError,
        };
      }
    } catch (e) {
      log(`  Error checking domain: ${e.message}`, 'warn');
      // Continue anyway, let the normal flow handle errors
    }
  }

  // Reconstruct visitedUrls from existing pages if no crawlState
  let visitedUrls;
  let toVisit;
  let savedRetryQueue = [];
  let processedTimestamps = []; // Rate limiting: track when each page was processed
  if (isResuming && previousState.crawlState?.visitedUrls) {
    // Have full crawlState - use it
    visitedUrls = new Set(previousState.crawlState.visitedUrls);
    toVisit = previousState.crawlState.toVisit || ['/'];
    savedRetryQueue = previousState.crawlState.retryQueue || [];
    processedTimestamps = previousState.crawlState.processedTimestamps || [];
    log(`  Resuming with crawlState: ${visitedUrls.size} visited, ${toVisit.length} queued, ${savedRetryQueue.length} in retry`, 'info');
  } else if (isResuming && previousState.pages?.length > 0) {
    // No crawlState but have pages - reconstruct visitedUrls from page paths
    visitedUrls = new Set(previousState.pages.map(p => p.path || new URL(p.url).pathname));
    toVisit = ['/']; // Start fresh queue, but visitedUrls will skip already-crawled pages
    log(`  Resuming WITHOUT crawlState: reconstructed ${visitedUrls.size} visited URLs from pages`, 'warn');
  } else {
    // Fresh start
    visitedUrls = new Set();
    toVisit = ['/'];
  }

  // Determine page limit:
  // 1. Per-job maxPages if set (0 = unlimited, positive = limit)
  // 2. Fall back to admin setting or unlimited domains
  const bgPrefix = isBackground ? '[BG] ' : '';
  let MAX_PAGES;
  if (jobMaxPages !== null && jobMaxPages !== undefined) {
    // Job has explicit limit set (background jobs always get 1000)
    MAX_PAGES = jobMaxPages === 0 ? Infinity : jobMaxPages;
    log(`  ${bgPrefix}Max pages: ${MAX_PAGES === Infinity ? 'unlimited' : MAX_PAGES}${isBackground ? ' (background job)' : ''}`, 'info');
  } else {
    // Use admin setting or check unlimited domains
    const isUnlimited = isUnlimitedDomain(urlObj.hostname);
    MAX_PAGES = isUnlimited || analyzeMaxPages === 0 ? Infinity : analyzeMaxPages;
    if (isUnlimited) {
      log(`  ${bgPrefix}Domain ${urlObj.hostname} has unlimited pages`, 'info');
    } else {
      log(`  ${bgPrefix}Max pages for ${urlObj.hostname}: ${MAX_PAGES}`, 'info');
    }
  }

  // Track pages processed in this chunk for round-robin
  let pagesProcessedThisChunk = 0;

  // Track if we stopped due to rate limit
  let rateLimitReached = false;
  let timeLimitReached = false;

  // Create a pool of pages for parallel processing
  const pagePool = [];
  for (let i = 0; i < PARALLEL_PAGES; i++) {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.setViewport({ width: 1920, height: 1080 });

    // Stealth: Hide webdriver property to avoid bot detection
    await page.evaluateOnNewDocument(() => {
      // Hide webdriver
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      // Add Chrome object
      window.chrome = { runtime: {} };
      // Fix permissions
      const originalQuery = window.navigator.permissions.query;
      window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
          ? Promise.resolve({ state: Notification.permission })
          : originalQuery(parameters)
      );
      // Hide automation
      Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
      Object.defineProperty(navigator, 'languages', { get: () => ['de-DE', 'de', 'en-US', 'en'] });
    });

    // Block ALL downloads - we only want to analyze pages, not download files
    const client = await page.createCDPSession();
    await client.send('Page.setDownloadBehavior', { behavior: 'deny' });
    page.on('console', () => {});
    page.on('pageerror', () => {});
    // Auto-dismiss all dialogs (alerts, confirms, prompts, beforeunload)
    page.on('dialog', async dialog => {
      try { await dialog.dismiss(); } catch {}
    });
    pagePool.push({ page, busy: false });
  }

  // Get an available page from pool
  const getAvailablePage = () => {
    return pagePool.find(p => !p.busy);
  };

  // Release page back to pool
  const releasePage = (poolItem) => {
    poolItem.busy = false;
  };

  // Process results from a page
  const processPageResult = (path, fullUrl, pageData) => {
    const pageResult = {
      url: fullUrl,
      path: path,
      images: [],
      stats: { total: 0, ob: 0, js: 0, noAlt: 0, emptyAlt: 0, unchanged: 0 },
    };

    for (const img of pageData.images) {
      // Count total files
      pageResult.stats.total++;
      result.summary.totalFiles++;

      // Track IMGFlow method (where it WOULD set the alt)
      if (img.imgflowMethod === 'ob') {
        pageResult.stats.ob++;
        result.summary.obProcessed++;
      } else {
        pageResult.stats.js++;
        result.summary.jsProcessed++;
      }

      // Track alt status for this file
      const fileHasAlt = img.hasAlt && !img.emptyAlt;
      if (!img.hasAlt && !img.emptyAlt) {
        pageResult.stats.noAlt++;
        result.summary.noAlt++;
      } else if (img.emptyAlt) {
        pageResult.stats.emptyAlt++;
        result.summary.emptyAlt++;
      } else {
        pageResult.stats.unchanged++;
        result.summary.unchanged++;
      }

      // Track unique images by normalized filename
      const normalizedName = normalizeImageUrl(img.src);
      if (!uniqueImagesMap.has(normalizedName)) {
        // First time seeing this base image
        uniqueImagesMap.set(normalizedName, {
          hasAlt: fileHasAlt,
          files: 1,
          firstSrc: img.src,
        });
      } else {
        // Already seen this base image - increment file count
        const existing = uniqueImagesMap.get(normalizedName);
        existing.files++;
        // If ANY variant has alt, mark image as having alt
        if (fileHasAlt) existing.hasAlt = true;
      }

      pageResult.images.push({
        src: img.src,
        alt: img.alt || '',
        hasAlt: img.hasAlt,
        method: img.imgflowMethod,
        width: img.width,
        height: img.height,
      });
    }

    // Update unique image counts from map
    result.summary.uniqueImages = uniqueImagesMap.size;
    result.summary.totalImages = uniqueImagesMap.size; // backwards compat
    result.summary.withAlt = [...uniqueImagesMap.values()].filter(v => v.hasAlt).length;
    result.summary.withoutAlt = uniqueImagesMap.size - result.summary.withAlt;

    result.pages.push(pageResult);

    // Add new links to queue (skip file URLs - PDFs, ZIPs, images, etc.)
    for (const link of pageData.links) {
      if (!visitedUrls.has(link) && !toVisit.includes(link) && !isFileUrl(link) && !isActionUrl(link)) {
        toVisit.push(link);
      }
    }

    // Update pagesQueued (total discovered URLs)
    result.pagesQueued = visitedUrls.size + toVisit.length;
  };

  // Active promises for parallel processing
  const activePromises = new Map();

  // Helper to adjust throttling (throttles on ANY error: timeout, connection, etc.)
  const adjustThrottling = (wasError, errorMessage = '') => {
    if (wasError) {
      consecutiveErrors++;
      successfulPages = 0;

      if (consecutiveErrors >= THROTTLE_ERROR_THRESHOLD && currentParallel > THROTTLE_MIN_PARALLEL) {
        const oldParallel = currentParallel;
        currentParallel = Math.max(THROTTLE_MIN_PARALLEL, Math.floor(currentParallel / 2));
        isThrottled = true;
        log(`  Throttling: ${consecutiveErrors} consecutive errors, reducing parallel from ${oldParallel} to ${currentParallel}`, 'warn');
      }

      // If 5+ consecutive errors at minimum parallelism, enter slow mode
      // We'll continue slowly instead of skipping (time limit will handle eventual release)
      if (consecutiveErrors >= 5 && currentParallel <= THROTTLE_MIN_PARALLEL && !isSlowMode) {
        isSlowMode = true;
        log(`  SLOW MODE: ${consecutiveErrors} consecutive errors - slowing down to 1 request per 30s`, 'warn');
      }
    } else {
      consecutiveErrors = 0;
      successfulPages++;

      // Slowly recover parallelism after successful pages
      if (isThrottled && successfulPages >= THROTTLE_RECOVERY_PAGES && currentParallel < PARALLEL_PAGES) {
        const oldParallel = currentParallel;
        currentParallel = Math.min(PARALLEL_PAGES, currentParallel + 1);
        successfulPages = 0;
        log(`  Recovering: increased parallel from ${oldParallel} to ${currentParallel}`, 'success');

        if (currentParallel >= PARALLEL_PAGES) {
          isThrottled = false;
          log(`  Fully recovered to ${PARALLEL_PAGES} parallel pages`, 'success');
        }
      }
    }
  };

  // Retry queue for failed pages (browser errors, not 404s)
  const retryQueue = savedRetryQueue.length > 0 ? [...savedRetryQueue] : [];
  const MAX_RETRIES = 2;
  const inFlight = new Set(); // Pages currently being processed
  if (retryQueue.length > 0) {
    log(`  Restored ${retryQueue.length} pages to retry queue`, 'info');
  }

  // If we've already reached or exceeded the page limit (e.g. from a resumed job),
  // clear queues to avoid infinite loop
  if (visitedUrls.size >= MAX_PAGES) {
    log(`  Already at/over page limit (${visitedUrls.size}/${MAX_PAGES}). Completing job...`, 'warn');
    toVisit.length = 0;
    retryQueue.length = 0;
  }

  // Track if we stopped due to chunk limit (not job completion)
  let chunkLimitReached = false;

  while ((toVisit.length > 0 && visitedUrls.size < MAX_PAGES) || activePromises.size > 0 || retryQueue.length > 0) {
    // Check if we've hit the chunk limit (round-robin mode)
    if (chunkSize !== Infinity && pagesProcessedThisChunk >= chunkSize && activePromises.size === 0) {
      chunkLimitReached = true;
      log(`  Chunk limit reached (${chunkSize} pages). Saving state for next worker...`, 'info');
      break;
    }

    // Check if we've hit the rate limit (pages per hour)
    if (hasRateLimit && activePromises.size === 0) {
      const oneHourAgo = Date.now() - (60 * 60 * 1000);
      const pagesInLastHour = processedTimestamps.filter(ts => ts > oneHourAgo).length;
      if (pagesInLastHour >= rateLimit) {
        rateLimitReached = true;
        log(`  Rate limit reached (${pagesInLastHour}/${rateLimit} pages/hour). Releasing job...`, 'warn');
        break;
      }
    }

    // Check if we've exceeded the time limit for this session (only in slow mode)
    if (isSlowMode && activePromises.size === 0) {
      const elapsed = Date.now() - jobSessionStart;
      if (elapsed >= MAX_JOB_TIME_MS) {
        log(`  TIME LIMIT: ${Math.round(elapsed / 60000)} min elapsed in slow mode - releasing job`, 'warn');
        timeLimitReached = true;
        break;
      }
    }

    // Respect current throttled parallelism
    const effectiveParallel = currentParallel;

    // Start new page analyses while we have pages in pool and URLs to visit
    // Don't start new pages if we've hit the chunk limit or rate limit (let active ones finish first)
    const chunkLimitHit = chunkSize !== Infinity && pagesProcessedThisChunk >= chunkSize;
    const rateLimitHit = hasRateLimit && processedTimestamps.filter(ts => ts > Date.now() - 60 * 60 * 1000).length >= rateLimit;
    while ((toVisit.length > 0 || retryQueue.length > 0) && visitedUrls.size < MAX_PAGES && activePromises.size < effectiveParallel && !chunkLimitHit && !rateLimitHit) {
      const poolItem = getAvailablePage();
      if (!poolItem) break; // No available pages, wait for one to finish

      // Prefer retry queue (failed pages get another chance)
      let pagePath, retryCount = 0;
      if (retryQueue.length > 0) {
        const retryItem = retryQueue.shift();
        pagePath = retryItem.path;
        retryCount = retryItem.retries;
      } else {
        pagePath = toVisit.shift();
      }

      if (!pagePath) break;
      if (visitedUrls.has(pagePath)) continue;
      if (inFlight.has(pagePath)) continue;
      // Skip file URLs and action URLs that might have snuck in from old checkpoints
      if (isFileUrl(pagePath) || isActionUrl(pagePath)) {
        visitedUrls.add(pagePath); // Mark as visited so we don't try again
        continue;
      }
      inFlight.add(pagePath); // Mark as in-flight, NOT as visited yet

      const fullUrl = url.replace(/\/$/, '') + pagePath;
      poolItem.busy = true;

      // Add delay when throttled or in slow mode
      if (isSlowMode) {
        log(`  [SLOW MODE] Waiting 30s before next request...`, 'warn');
        await new Promise(r => setTimeout(r, SLOW_MODE_DELAY_MS));
      } else if (isThrottled && currentParallel <= 2) {
        await new Promise(r => setTimeout(r, THROTTLE_DELAY_MS));
      }

      const parallelInfo = isThrottled
        ? `${activePromises.size + 1}/${effectiveParallel} parallel, throttled from ${PARALLEL_PAGES}`
        : `${activePromises.size + 1}/${effectiveParallel} parallel`;
      log(`  Analyzing: ${pagePath} (${parallelInfo})`, 'info');

      // Start async analysis
      const promise = (async () => {
        try {
          const pageData = await analyzeSinglePage(poolItem.page, fullUrl, urlObj, basicAuth);

          // Take screenshot on first page, then every N pages
          const currentPageNum = pageCounter++;
          const shouldTakeScreenshot = currentPageNum === 0 || (currentPageNum % SCREENSHOT_EVERY_N_PAGES === 0);

          if (shouldTakeScreenshot) {
            try {
              // Wait for animations and fade-ins to complete
              await new Promise(r => setTimeout(r, ANIMATION_WAIT));

              // Scroll to top and wait for any lazy-loaded content
              await poolItem.page.evaluate(() => window.scrollTo(0, 0));
              await new Promise(r => setTimeout(r, 2000));

              // Keep desktop viewport (1920x1080) for proper desktop rendering
              const screenshotFilename = `page-${currentPageNum}.jpg`;

              // Take screenshot as buffer (desktop view)
              const screenshotBuffer = await poolItem.page.screenshot({
                type: 'jpeg',
                quality: 70,
                fullPage: false,
              });

              // Upload to IMGFlow server
              const screenshotUrl = await uploadScreenshot(jobId, screenshotFilename, screenshotBuffer);

              result.screenshots.push({
                url: screenshotUrl,
                pageNum: currentPageNum,
                pagePath: pagePath,
              });

              // Always update main screenshot to latest (backwards compatibility)
              result.screenshot = screenshotUrl;

              log(`  Screenshot ${result.screenshots.length} uploaded: page ${currentPageNum} (${pagePath})`, 'info');
            } catch (screenshotError) {
              log(`  Screenshot failed: ${screenshotError.message}`, 'warn');
            }
          }

          return { pagePath, fullUrl, pageData, poolItem, success: true, retryCount };
        } catch (error) {
          log(`  Error on ${pagePath}: ${error.message}`, 'warn');
          return { pagePath, fullUrl, pageData: { images: [], links: [] }, poolItem, error, success: false, retryCount };
        }
      })();

      activePromises.set(pagePath, promise);
    }

    // Wait for at least one to complete if we have active promises
    if (activePromises.size > 0) {
      const completedPromise = await Promise.race(activePromises.values());
      activePromises.delete(completedPromise.pagePath);

      // Release page back to pool
      releasePage(completedPromise.poolItem);

      // Remove from in-flight set
      inFlight.delete(completedPromise.pagePath);

      // Check for "detached Frame" error - means browser is corrupted
      const errorMsg = completedPromise.error?.message || '';
      const isRetriableError = errorMsg.includes('detached Frame') ||
                               errorMsg.includes('Target closed') ||
                               errorMsg.includes('Session closed') ||
                               errorMsg.includes('net::ERR_') ||
                               errorMsg.includes('Navigation timeout') ||
                               errorMsg.includes('Timeout');

      if (errorMsg.includes('detached Frame') || errorMsg.includes('Target closed') || errorMsg.includes('Session closed')) {
        consecutiveDetachedErrors = (consecutiveDetachedErrors || 0) + 1;
        if (consecutiveDetachedErrors >= 5) {
          log(`  FATAL: Browser corrupted (${consecutiveDetachedErrors} detached frame errors). Requesting restart...`, 'error');
          throw new Error('BROWSER_CORRUPTED');
        }
      } else if (completedPromise.success) {
        consecutiveDetachedErrors = 0; // Reset on success
      }

      // Adjust throttling based on success/failure
      adjustThrottling(!completedPromise.success, errorMsg);

      // Handle result
      if (completedPromise.success) {
        // SUCCESS: Mark as visited and process results
        visitedUrls.add(completedPromise.pagePath);
        processPageResult(completedPromise.pagePath, completedPromise.fullUrl, completedPromise.pageData);
        pagesProcessedThisChunk++; // Increment chunk counter
        processedTimestamps.push(Date.now()); // Rate limiting: track when this page was processed

        // Save crawl state periodically (every N pages, same as screenshots) for resume capability
        const shouldSaveState = result.pages.length % SCREENSHOT_EVERY_N_PAGES === 0;
        if (shouldSaveState) {
          // Clean old timestamps (only keep last hour for rate limiting)
          const oneHourAgo = Date.now() - (60 * 60 * 1000);
          const recentTimestamps = processedTimestamps.filter(ts => ts > oneHourAgo);

          result.crawlState = {
            visitedUrls: [...visitedUrls],
            toVisit: [...toVisit],
            retryQueue: retryQueue.map(r => ({ path: r.path, retries: r.retries })),
            pageCounter: pageCounter,
            processedTimestamps: recentTimestamps, // Rate limiting
          };
          log(`  Checkpoint saved at page ${result.pages.length}`, 'info');
        }

        // Send live update on every page (minimal data, no pages array)
        if (onProgress) {
          await onProgress(result);
        }
      } else if (isRetriableError && completedPromise.retryCount < MAX_RETRIES) {
        // RETRIABLE ERROR: Add to retry queue
        retryQueue.push({ path: completedPromise.pagePath, retries: completedPromise.retryCount + 1 });
        log(`  Queued for retry (${completedPromise.retryCount + 1}/${MAX_RETRIES}): ${completedPromise.pagePath}`, 'warn');
      } else {
        // NON-RETRIABLE ERROR or max retries reached: Mark as visited (skip)
        visitedUrls.add(completedPromise.pagePath);
        if (completedPromise.retryCount >= MAX_RETRIES) {
          log(`  Max retries reached, skipping: ${completedPromise.pagePath}`, 'error');
        }
      }
    }
  }

  // Determine if job is truly complete or just paused for round-robin/rate-limit
  const hasMoreWork = (toVisit.length > 0 || retryQueue.length > 0) && visitedUrls.size < MAX_PAGES;
  let isComplete = !chunkLimitReached && !rateLimitReached && !timeLimitReached && !hasMoreWork;

  // SAFETY: Never mark a job as "complete" with 0 pages - something went wrong
  // This prevents losing progress when all pages fail in a session
  if (isComplete && result.pages.length === 0) {
    log(`  WARNING: Would complete with 0 pages - marking as failed instead`, 'error');
    result.isComplete = false;
    result.crawlState = null; // No state to save
    result.error = 'All pages failed to load';
    isComplete = false;
  }

  if (isComplete) {
    // Job is fully complete - clear crawl state
    result.crawlState = null;
    result.isComplete = true;
    log(`  Analysis complete: ${result.pages.length} pages, ${result.summary.uniqueImages} images`, 'success');
  } else {
    // Job paused for round-robin OR hit page limit - save state for resume
    // Clean old timestamps (only keep last hour for rate limiting)
    const oneHourAgo = Date.now() - (60 * 60 * 1000);
    const recentTimestamps = processedTimestamps.filter(ts => ts > oneHourAgo);

    result.crawlState = {
      visitedUrls: [...visitedUrls],
      toVisit: [...toVisit],
      retryQueue: retryQueue.map(r => ({ path: r.path, retries: r.retries })),
      pageCounter: pageCounter,
      processedTimestamps: recentTimestamps, // Rate limiting
    };
    result.isComplete = false;

    if (timeLimitReached) {
      const elapsed = Math.round((Date.now() - jobSessionStart) / 60000);
      log(`  Time limit reached (${elapsed} min in slow mode). ${result.pages.length} pages done, ${toVisit.length} queued. Releasing...`, 'warn');
    } else if (rateLimitReached) {
      const oneHourAgo = Date.now() - (60 * 60 * 1000);
      const pagesInLastHour = processedTimestamps.filter(ts => ts > oneHourAgo).length;
      log(`  Rate limited: ${pagesInLastHour}/${rateLimit} pages/hour. ${toVisit.length} URLs queued. Releasing...`, 'warn');
    } else if (chunkLimitReached) {
      log(`  Chunk done: ${pagesProcessedThisChunk} pages processed, ${toVisit.length} queued. Releasing for next worker...`, 'info');
    } else if (visitedUrls.size >= MAX_PAGES) {
      log(`  Reached page limit (${MAX_PAGES}). ${toVisit.length} URLs remaining in queue.`, 'warn');
      result.isComplete = true; // Page limit reached = job is done

      // Save crawl state locally if there are remaining URLs (for potential manual continuation)
      if (toVisit.length > 0) {
        const stateDir = path.join(process.cwd(), 'crawl-states');
        if (!fs.existsSync(stateDir)) {
          fs.mkdirSync(stateDir, { recursive: true });
        }
        const hostname = urlObj.hostname.replace(/^www\./, '');
        const stateFile = path.join(stateDir, `${hostname}.json`);
        const savedState = {
          jobId,
          url,
          hostname,
          visitedUrls: [...visitedUrls],
          toVisit: [...toVisit],
          retryQueue: retryQueue.map(r => ({ path: r.path, retries: r.retries })),
          savedAt: new Date().toISOString(),
          pagesAnalyzed: visitedUrls.size,
          remainingUrls: toVisit.length
        };
        try {
          fs.writeFileSync(stateFile, JSON.stringify(savedState, null, 2));
          log(`  Saved crawl state locally: ${stateFile} (${toVisit.length} URLs remaining)`, 'info');
        } catch (e) {
          log(`  Warning: Could not save crawl state: ${e.message}`, 'warn');
        }
      }
      result.crawlState = null; // Don't save in DB
    }
  }

  // Close all pool pages
  for (const poolItem of pagePool) {
    await poolItem.page.close();
  }

  return result;
}

async function processJob(browser, job) {
  // SAFETY CHECK: Verify this worker still owns the job before processing
  // This prevents race conditions where two workers claim the same job
  const currentJob = await withRetry(() => prisma.analyzeJob.findUnique({
    where: { id: job.id },
    select: { workerId: true, status: true },
  }));

  if (currentJob?.workerId && currentJob.workerId !== WORKER_ID) {
    log(`Job ${job.id.substring(0, 8)} already claimed by ${currentJob.workerId}, skipping...`, 'warn');
    return; // Another worker got it, skip
  }

  if (currentJob?.status === 'completed' || currentJob?.status === 'cancelled') {
    log(`Job ${job.id.substring(0, 8)} already ${currentJob.status}, skipping...`, 'warn');
    return;
  }

  // Check if job has previous state (was interrupted and can be resumed)
  // Resume if: has crawlState OR has existing pages (interrupted between checkpoints)
  const existingPages = job.result?.pages?.length || 0;
  const hasCrawlState = !!job.result?.crawlState;
  const previousState = (hasCrawlState || existingPages > 0) ? job.result : null;
  const isResuming = !!previousState;

  // Check if job was skipped too many times due to consecutive errors
  const errorSkips = job.result?.crawlState?.consecutiveErrorSkips || 0;
  if (errorSkips >= 3) {
    log(`Job ${job.id.substring(0, 8)} failed: skipped ${errorSkips}x due to consecutive errors`, 'error');
    if (job.priority === 1) {
      // Background job: delete and mark as skipped
      const hostname = new URL(job.url).hostname.replace(/^www\./, '');
      await withRetry(() => prisma.analyzeJob.delete({ where: { id: job.id } }));
      await withRetry(() => prisma.backgroundDomainQueue.updateMany({
        where: { domain: hostname },
        data: { status: 'skipped' }
      }));
      log(`Background job deleted, domain ${hostname} marked as skipped`, 'info');
    } else {
      // Normal job: handle with auto-retry
      await handleJobFailure(job, `Site broken or blocking: skipped ${errorSkips}x due to consecutive errors`, 'unreachable');
    }
    return;
  }

  if (isResuming) {
    log(`Resuming job ${job.id.substring(0, 8)}... URL: ${job.url} (${job.result.pages?.length || 0} pages done)`, 'info');
  } else {
    log(`Processing job ${job.id.substring(0, 8)}... URL: ${job.url} (${PARALLEL_PAGES} parallel pages)`, 'info');
  }

  try {
    // Mark job as processing (with retry)
    await withRetry(() => prisma.analyzeJob.update({
      where: { id: job.id },
      data: {
        status: 'processing',
        startedAt: isResuming ? job.startedAt : new Date(), // Keep original start time if resuming
        workerId: WORKER_ID,
      },
    }));

    // Track if job was cancelled
    let cancelled = false;

    // Run analysis with live updates callback, passing previous state if resuming
    // Pass chunk size for round-robin, per-job maxPages, and rate limit
    const result = await analyzeWebsite(
      browser,
      job.url,
      job.basicAuth,
      job.id,
      async (partialResult) => {
        // Check if job was cancelled (with retry for connection issues)
        const currentJob = await withRetry(() => prisma.analyzeJob.findUnique({
          where: { id: job.id },
          select: { status: true },
        }));

        if (currentJob?.status === 'cancelled') {
          cancelled = true;
          throw new Error('Job cancelled by user');
        }

        // Calculate stats
        const uniqueImages = partialResult.summary.uniqueImages;
        const withAlt = partialResult.summary.withAlt;
        const withoutAlt = partialResult.summary.withoutAlt;
        const score = uniqueImages > 0 ? Math.round((withAlt / uniqueImages) * 100) : 100;

        // LOCAL STORAGE: Save full result locally (not to database)
        // This avoids sending large result objects over the network during analysis
        saveLocalResult(job.id, partialResult);

        // Save stats + minimal result for live progress display
        // Include screenshots and pagesQueued but NOT the full pages array (too large)
        const liveResult = {
          pagesQueued: partialResult.pagesQueued,
          screenshots: partialResult.screenshots,
          summary: partialResult.summary,
          // Don't include pages array - use pagesCount field instead
        };

        await withRetry(() => prisma.analyzeJob.update({
          where: { id: job.id },
          data: {
            result: liveResult, // Minimal result for live progress
            pagesCount: partialResult.pages.length, // Separate field for page count
            totalImages: uniqueImages,
            withAlt: withAlt,
            withoutAlt: withoutAlt,
            obProcessed: partialResult.summary.obProcessed,
            jsProcessed: partialResult.summary.jsProcessed,
            score: score,
          },
        }));
      },
      previousState,
      {
        chunkSize: roundRobinChunkSize,
        // Background jobs (priority=1) always use maxPages=200
        jobMaxPages: job.priority === 1 ? bgMaxPages : job.maxPages,
        jobPagesPerHour: job.pagesPerHour,
        isBackground: job.priority === 1
      }
    );

    // Check if domain is dead/unreachable (early exit from analyzeWebsite)
    if (result.domainStatus && result.domainStatus !== 'alive') {
      if (job.priority === 1) {
        // Background job: Delete the AnalyzeJob and mark domain as skipped in queue
        const hostname = new URL(job.url).hostname.replace(/^www\./, '');
        await withRetry(() => prisma.analyzeJob.delete({ where: { id: job.id } }));
        await withRetry(() => prisma.backgroundDomainQueue.updateMany({
          where: { domain: hostname },
          data: { status: 'skipped' }
        }));
        log(`Background job ${job.id.substring(0, 8)} skipped: ${hostname} is ${result.domainStatus}`, 'info');
      } else {
        // Normal job: handle with auto-retry (domain issues might be temporary)
        await handleJobFailure(job, result.error || `Domain is ${result.domainStatus}`, result.domainStatus);
      }
      return; // Don't trigger AI categorization for dead domains
    }

    // Check if analysis failed with 0 pages (all pages failed to load)
    if (result.error && result.pages.length === 0) {
      log(`Job ${job.id.substring(0, 8)} failed: ${result.error}`, 'error');
      await handleJobFailure(job, result.error, 'unreachable');
      return;
    }

    // Use pre-calculated unique image stats from summary
    const uniqueImages = result.summary.uniqueImages;
    const withAlt = result.summary.withAlt;
    const withoutAlt = result.summary.withoutAlt;
    const score = uniqueImages > 0 ? Math.round((withAlt / uniqueImages) * 100) : 100;

    // SAFETY: Final check - never save fewer pages than already exist
    const finalCheck = await withRetry(() => prisma.analyzeJob.findUnique({
      where: { id: job.id },
      select: { result: true },
    }));
    const existingFinalPages = finalCheck?.result?.pages?.length || 0;
    const newFinalPages = result.pages?.length || 0;

    if (newFinalPages < existingFinalPages) {
      log(`SAFETY: Cannot complete - would lose pages (${existingFinalPages} -> ${newFinalPages}). Keeping existing data.`, 'error');
      return; // Don't overwrite, keep existing data
    }

    // Check if job is complete or just paused for round-robin
    if (result.isComplete) {
      // JOB COMPLETE: Upload full result to database (only time we send full data!)
      log(`Uploading complete result (${result.pages.length} pages, ${JSON.stringify(result).length} bytes)...`, 'info');

      await withRetry(() => prisma.analyzeJob.update({
        where: { id: job.id },
        data: {
          status: 'completed',
          completedAt: new Date(),
          result: result, // Full result uploaded only at completion!
          pagesCount: result.pages.length, // Cached for fast queries
          totalImages: uniqueImages,
          withAlt: withAlt,
          withoutAlt: withoutAlt,
          obProcessed: result.summary.obProcessed,
          jsProcessed: result.summary.jsProcessed,
          score: score,
          workerId: null, // Release worker
        },
      }));

      // Keep local file as backup (in LOCAL_RESULTS_DIR)
      log(`Completed job ${job.id.substring(0, 8)}: ${uniqueImages} unique images (${result.summary.totalFiles} files), ${result.pages.length} pages, score ${score}%`, 'success');
      log(`  Local backup: ${getLocalResultPath(job.id)}`, 'info');

      // Send email notification if requested
      const updatedJob = await withRetry(() => prisma.analyzeJob.findUnique({ where: { id: job.id } }));
      if (updatedJob?.notifyEmail) {
        await sendNotificationEmail(updatedJob, result);
      }

      // Trigger AI categorization (async, don't wait)
      triggerCategorization(job.id);
    } else {
      // JOB PAUSED: Release for next worker (round-robin)
      // Upload checkpoint with crawlState so next worker can resume
      log(`Uploading checkpoint (${result.pages.length} pages) for next worker...`, 'info');

      await withRetry(() => prisma.analyzeJob.update({
        where: { id: job.id },
        data: {
          result: result, // Save crawlState for resume
          pagesCount: result.pages.length, // Cached for fast queries
          totalImages: uniqueImages,
          withAlt: withAlt,
          withoutAlt: withoutAlt,
          obProcessed: result.summary.obProcessed,
          jsProcessed: result.summary.jsProcessed,
          score: score,
          workerId: null, // Release - next worker will pick it up
        },
      }));

      const releaseReason = result.skipReason === 'consecutive_errors'
        ? `(consecutive errors, skip #${result.crawlState?.consecutiveErrorSkips || 1})`
        : `after ${roundRobinChunkSize} pages`;
      log(`Released job ${job.id.substring(0, 8)} ${releaseReason}. Total: ${result.pages.length} pages. Queued: ${result.crawlState?.toVisit?.length || 0}`, 'info');
      log(`  Local backup: ${getLocalResultPath(job.id)}`, 'info');
    }

  } catch (error) {
    // Check if job was cancelled
    if (error.message === 'Job cancelled by user') {
      log(`Job ${job.id.substring(0, 8)} cancelled by user`, 'warn');
      // Status is already 'cancelled' from the API, no need to update
    } else {
      log(`Failed job ${job.id.substring(0, 8)}: ${error.message}`, 'error');
      await handleJobFailure(job, error.message);
    }
  }
}

// Helper to try claiming jobs from a candidate list
async function tryClaimFromCandidates(candidates, isBackground = false) {
  const prefix = isBackground ? '[BG] ' : '';

  for (const candidate of candidates) {
    const rateCheck = checkJobRateLimit(candidate);
    if (rateCheck.limited) {
      const hostname = candidate.result?.pages?.[0]?.url ? new URL(candidate.result.pages[0].url).hostname : candidate.id.substring(0, 8);
      log(`${prefix}Skipping rate-limited job ${hostname}: ${rateCheck.pagesInLastHour}/${rateCheck.limit} pages/hour (wait ${rateCheck.waitMinutes}min)`, 'info');
      continue; // Skip to next candidate
    }

    // This job is not rate-limited, claim it
    if (candidate.status === 'processing') {
      // Released job - just claim it
      const claimed = await withRetry(() => prisma.$queryRaw`
        UPDATE "AnalyzeJob"
        SET "workerId" = ${WORKER_ID}, "updatedAt" = NOW()
        WHERE id = ${candidate.id}
        RETURNING *
      `);
      if (claimed[0]) {
        const pagesProcessed = claimed[0].result?.pages?.length || 0;
        const queued = claimed[0].result?.crawlState?.toVisit?.length || 0;
        log(`${prefix}Resuming released job ${claimed[0].id.substring(0, 8)}: ${pagesProcessed} pages done, ${queued} queued`, 'info');
        return claimed[0];
      }
    } else {
      // Pending job - start it
      const claimed = await withRetry(() => prisma.$queryRaw`
        UPDATE "AnalyzeJob"
        SET status = 'processing', "startedAt" = NOW(), "workerId" = ${WORKER_ID}
        WHERE id = ${candidate.id}
        RETURNING *
      `);
      if (claimed[0]) {
        log(`${prefix}Starting new job ${claimed[0].id.substring(0, 8)}: ${new URL(claimed[0].url).hostname}`, 'info');
        return claimed[0];
      }
    }
  }
  return null; // All were rate-limited or couldn't be claimed
}

async function claimNextJob() {
  // PRIORITY SCHEDULING with RATE LIMITING:
  // 1. Normal jobs (priority=0) first
  // 2. Background jobs (priority=1) only when ALL normal jobs are rate-limited or none exist

  // FIRST: Check for abandoned NORMAL jobs (worker died) - highest priority
  const abandonedNormal = await withRetry(() => prisma.$queryRaw`
    UPDATE "AnalyzeJob"
    SET "workerId" = ${WORKER_ID}, "updatedAt" = NOW(), "startedAt" = COALESCE("startedAt", NOW())
    WHERE id = (
      SELECT id FROM "AnalyzeJob"
      WHERE status = 'processing'
        AND "workerId" IS NOT NULL
        AND "updatedAt" < NOW() - INTERVAL '10 minutes'
        AND priority = 0
      ORDER BY "updatedAt" ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *
  `);

  if (abandonedNormal[0]) {
    const rateCheck = checkJobRateLimit(abandonedNormal[0]);
    if (rateCheck.limited) {
      log(`Abandoned job ${abandonedNormal[0].id.substring(0, 8)} is rate-limited. Releasing...`, 'warn');
      await withRetry(() => prisma.analyzeJob.update({
        where: { id: abandonedNormal[0].id },
        data: { workerId: null },
      }));
    } else {
      const hasCheckpoint = abandonedNormal[0].result?.crawlState;
      log(`Found abandoned job ${abandonedNormal[0].id.substring(0, 8)} ${hasCheckpoint ? 'with checkpoint' : 'without checkpoint'}`, 'warn');
      return abandonedNormal[0];
    }
  }

  // SECOND: Try normal jobs (priority = 0)
  const normalCandidates = await withRetry(() => prisma.$queryRaw`
    SELECT id, status, "updatedAt", "createdAt", "pagesPerHour", priority, result,
           CASE WHEN status = 'processing' THEN "updatedAt" ELSE "createdAt" END as wait_since
    FROM "AnalyzeJob"
    WHERE ((status = 'processing' AND "workerId" IS NULL) OR status = 'pending')
      AND priority = 0
    ORDER BY wait_since ASC
    LIMIT 10
    FOR UPDATE SKIP LOCKED
  `);

  if (normalCandidates && normalCandidates.length > 0) {
    const claimed = await tryClaimFromCandidates(normalCandidates, false);
    if (claimed) return claimed;

    // All normal jobs are rate-limited - now check background jobs
    log(`All ${normalCandidates.length} normal jobs are rate-limited, checking background jobs...`, 'info');
  }

  // THIRD: Try background jobs (priority = 1) - only when normal queue is empty or rate-limited
  const bgCandidates = await withRetry(() => prisma.$queryRaw`
    SELECT id, status, "updatedAt", "createdAt", "pagesPerHour", priority, result,
           CASE WHEN status = 'processing' THEN "updatedAt" ELSE "createdAt" END as wait_since
    FROM "AnalyzeJob"
    WHERE ((status = 'processing' AND "workerId" IS NULL) OR status = 'pending')
      AND priority = 1
    ORDER BY wait_since ASC
    LIMIT 10
    FOR UPDATE SKIP LOCKED
  `);

  if (bgCandidates && bgCandidates.length > 0) {
    const claimed = await tryClaimFromCandidates(bgCandidates, true);
    if (claimed) return claimed;

    log(`All ${bgCandidates.length} background jobs are also rate-limited, waiting...`, 'info');
  } else if (normalCandidates && normalCandidates.length > 0) {
    log(`All ${normalCandidates.length} jobs are rate-limited, waiting...`, 'info');
  }

  // FOURTH: No jobs available - check if we should pull from BackgroundDomainQueue
  // Only pull if < 100 background jobs are currently active
  const MAX_BACKGROUND_JOBS = bgMaxJobs;
  const activeBackgroundJobs = await withRetry(() => prisma.analyzeJob.count({
    where: {
      priority: 1,
      status: { in: ['pending', 'processing'] }
    }
  }));

  if (activeBackgroundJobs < MAX_BACKGROUND_JOBS) {
    // Pull random domain from queue
    const nextDomain = await withRetry(() => prisma.$queryRaw`
      UPDATE "BackgroundDomainQueue"
      SET status = 'imported', "importedAt" = NOW()
      WHERE id = (
        SELECT id FROM "BackgroundDomainQueue"
        WHERE status = 'pending'
        ORDER BY RANDOM()
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    `);

    if (nextDomain && nextDomain[0]) {
      const domain = nextDomain[0].domain;
      const url = domain.startsWith('http') ? domain : `https://${domain}`;

      // Create new background AnalyzeJob
      const newJob = await withRetry(() => prisma.analyzeJob.create({
        data: {
          url,
          priority: 1,
          maxPages: bgMaxPages,
          status: 'processing',
          startedAt: new Date(),
          workerId: WORKER_ID
        }
      }));

      log(`[BG] Pulled from queue: ${domain} (${activeBackgroundJobs + 1}/${MAX_BACKGROUND_JOBS} active)`, 'info');
      return newJob;
    }
  }

  return null;
}

async function runWorker() {
  console.log(`\n${c.bright}${c.cyan}IMGFlow Analyze Worker${c.reset}\n`);
  console.log(`${c.gray}Worker ID:${c.reset}        ${c.cyan}${WORKER_ID}${c.reset}`);
  console.log(`${c.gray}Parallel Pages:${c.reset}   ${c.yellow}${PARALLEL_PAGES}${c.reset} (use --parallel N to change)`);
  console.log(`${c.gray}Round-Robin:${c.reset}      ${c.green}${roundRobinChunkSize}${c.reset} pages per job, then next job`);
  console.log(`${c.gray}Rate Limit:${c.reset}       ${c.yellow}${analyzePagesPerHour}${c.reset} pages/hour per job (0 = unlimited)`);
  console.log(`${c.gray}Screenshots:${c.reset}      Every ${c.yellow}${SCREENSHOT_EVERY_N_PAGES}${c.reset} pages (use --screenshot-every N to change)`);
  console.log(`${c.gray}Background:${c.reset}       Max ${c.yellow}${bgMaxPages}${c.reset} pages, max ${c.yellow}${bgMaxJobs}${c.reset} active jobs`);
  console.log(`${c.gray}User-Agent:${c.reset}       Chrome 120 (Windows)`);
  console.log(`${c.gray}CPU Cores:${c.reset}        ${os.cpus().length}`);
  console.log(`${c.gray}Memory:${c.reset}           ${Math.round(os.totalmem() / 1024 / 1024 / 1024)}GB total, ${Math.round(os.freemem() / 1024 / 1024 / 1024)}GB free`);
  console.log(`${c.gray}Mem Cleanup:${c.reset}      Browser restart every ${c.yellow}10${c.reset} jobs`);
  console.log('');

  log(`Starting worker...`, 'info');

  // Helper to launch/restart browser with memory-optimized args
  const launchBrowser = async () => {
    const b = await puppeteer.launch({
      headless: 'new',
      protocolTimeout: 120000, // 2 minutes timeout for browser commands
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        // Memory optimization args
        '--disable-gpu',
        '--disable-extensions',
        '--disable-background-networking',
        '--disable-default-apps',
        '--disable-sync',
        '--disable-translate',
        '--metrics-recording-only',
        '--no-first-run',
        '--safebrowsing-disable-auto-update',
        // Disable all popups, notifications, permission prompts
        '--disable-notifications',
        '--disable-popup-blocking',
        '--disable-infobars',
        '--disable-features=TranslateUI',
        '--deny-permission-prompts',
        '--disable-geolocation',
        '--disable-background-timer-throttling',
        '--autoplay-policy=no-user-gesture-required',
        '--js-flags=--max-old-space-size=512', // Limit JS heap to 512MB
      ],
    });
    log(`Browser ready (Puppeteer)`, 'success');
    return b;
  };

  // Track jobs for periodic restart
  let jobsSinceBrowserRestart = 0;
  const RESTART_BROWSER_EVERY_N_JOBS = 10; // Restart browser every N jobs to free memory

  // Launch browser (mutable for restart)
  let browser = await launchBrowser();

  // Handle graceful shutdown
  let running = true;
  const shutdown = async () => {
    log('Shutting down...', 'warn');
    running = false;
    await browser.close();
    await prisma.$disconnect();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // Main loop
  while (running) {
    try {
      const job = await claimNextJob();

      if (job) {
        await processJob(browser, job);
        jobsSinceBrowserRestart++;

        // Periodic browser restart to free accumulated memory
        if (jobsSinceBrowserRestart >= RESTART_BROWSER_EVERY_N_JOBS) {
          const memUsage = process.memoryUsage();
          const heapMB = Math.round(memUsage.heapUsed / 1024 / 1024);
          log(`Memory cleanup: ${heapMB}MB heap used after ${jobsSinceBrowserRestart} jobs. Restarting browser...`, 'info');
          try { await browser.close(); } catch (e) { /* ignore */ }
          if (global.gc) global.gc(); // Force garbage collection if available
          browser = await launchBrowser();
          jobsSinceBrowserRestart = 0;
        }
      } else {
        // No jobs, wait before polling again
        await new Promise(r => setTimeout(r, POLL_INTERVAL));
      }
    } catch (error) {
      // Check if browser is corrupted and needs restart
      if (error.message === 'BROWSER_CORRUPTED' || error.message?.includes('detached Frame') || error.message?.includes('Target closed') || error.message?.includes('timed out') || error.message?.includes('ProtocolError')) {
        log(`Browser corrupted or timed out, restarting...`, 'warn');
        try {
          await browser.close();
        } catch (e) {
          // Browser might already be dead, ignore
        }
        browser = await launchBrowser();
        log(`Browser restarted, continuing...`, 'success');
      } else {
        log(`Error in main loop: ${error.message}`, 'error');
      }
      await new Promise(r => setTimeout(r, POLL_INTERVAL * 2));
    }
  }
}

// Check for stale jobs on startup (jobs stuck in processing with NO progress)
async function cleanupStaleJobs() {
  const staleThreshold = new Date(Date.now() - 10 * 60 * 1000); // 10 minutes

  // Only reset jobs that have NO progress (0 images) - active jobs should not be touched
  const staleCount = await withRetry(() => prisma.analyzeJob.updateMany({
    where: {
      status: 'processing',
      startedAt: { lt: staleThreshold },
      OR: [
        { totalImages: null },
        { totalImages: 0 },
      ],
    },
    data: {
      status: 'pending',
      startedAt: null,
      workerId: null,
    },
  }));

  if (staleCount.count > 0) {
    log(`Reset ${staleCount.count} stale jobs (no progress)`, 'warn');
  }
}

// Interactive pick mode: show pending jobs and let user select one
async function runPickMode() {
  const readline = require('readline');

  console.log(`\n${c.bright}${c.cyan}IMGFlow Job Picker${c.reset}\n`);

  // Fetch pending and processing jobs
  const jobs = await withRetry(() => prisma.analyzeJob.findMany({
    where: {
      status: { in: ['pending', 'processing'] }
    },
    orderBy: { createdAt: 'asc' },
    select: {
      id: true,
      url: true,
      status: true,
      createdAt: true,
      totalImages: true,
      result: true,
    }
  }));

  if (jobs.length === 0) {
    console.log(`${c.yellow}No pending or processing jobs found.${c.reset}`);
    process.exit(0);
  }

  // Display jobs
  console.log(`${c.gray}Available jobs:${c.reset}\n`);
  jobs.forEach((job, i) => {
    const hostname = new URL(job.url).hostname;
    const pages = job.result?.pages?.length || 0;
    const status = job.status === 'processing'
      ? `${c.yellow}processing${c.reset} (${pages} pages)`
      : `${c.green}pending${c.reset}`;
    console.log(`  ${c.cyan}[${i + 1}]${c.reset} ${hostname} - ${status}`);
  });

  // Prompt for selection
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  rl.question(`\n${c.gray}Enter number (1-${jobs.length}) or 'q' to quit:${c.reset} `, async (answer) => {
    rl.close();

    if (answer.toLowerCase() === 'q') {
      console.log('Bye!');
      process.exit(0);
    }

    const idx = parseInt(answer, 10) - 1;
    if (isNaN(idx) || idx < 0 || idx >= jobs.length) {
      console.error(`${c.red}Invalid selection${c.reset}`);
      process.exit(1);
    }

    const selectedJob = jobs[idx];
    console.log(`\n${c.green}Selected: ${new URL(selectedJob.url).hostname}${c.reset}\n`);

    // Helper to launch browser
    const launchBrowser = async () => {
      log(`Launching browser...`, 'info');
      const b = await puppeteer.launch({
        headless: 'new',
        protocolTimeout: 120000, // 2 minutes timeout for browser commands
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-notifications',
          '--disable-popup-blocking',
          '--disable-infobars',
          '--deny-permission-prompts',
          '--disable-geolocation',
        ],
      });
      log(`Browser ready (${PARALLEL_PAGES} parallel pages)`, 'success');
      return b;
    };

    let browser = await launchBrowser();
    let retries = 0;
    const maxRetries = 3;

    // Handle graceful shutdown
    const shutdown = async () => {
      log('Shutting down...', 'warn');
      await browser.close();
      await prisma.$disconnect();
      process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    // Fetch full job data and process with retry on browser corruption
    while (retries < maxRetries) {
      try {
        const fullJob = await prisma.analyzeJob.findUnique({ where: { id: selectedJob.id } });
        await processJob(browser, fullJob);
        break; // Success, exit loop
      } catch (error) {
        if (error.message === 'BROWSER_CORRUPTED' || error.message?.includes('detached Frame') || error.message?.includes('Target closed') || error.message?.includes('timed out') || error.message?.includes('ProtocolError')) {
          retries++;
          log(`Browser corrupted or timed out, restarting (attempt ${retries}/${maxRetries})...`, 'warn');
          try { await browser.close(); } catch (e) { /* ignore */ }
          if (retries < maxRetries) {
            browser = await launchBrowser();
          }
        } else {
          throw error; // Other errors, don't retry
        }
      }
    }

    await browser.close();
    await prisma.$disconnect();
    process.exit(0);
  });
}

// Start
if (PICK_MODE) {
  // Interactive pick mode - also fetch settings first!
  fetchSettings()
    .then(() => runPickMode())
    .catch(e => {
      console.error(`${c.red}Fatal error: ${e.message}${c.reset}`);
      process.exit(1);
    });
} else {
  // Queue worker mode
  cleanupStaleJobs()
    .then(() => fetchSettings())
    .then(() => runWorker())
    .catch(e => {
      console.error(`${c.red}Fatal error: ${e.message}${c.reset}`);
      process.exit(1);
    });
}
