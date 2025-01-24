import { Headers } from 'headers.js';

/**
 * Implementation of the Fetch API that wraps the Rust fetch implementation
 */

/**
 * @typedef {Object} RequestInit
 * @property {string} [method='GET'] - HTTP method
 * @property {Object} [headers] - Request headers
 * @property {string} [body] - Request body
 * @property {number} [timeout] - Request timeout in milliseconds
 */

/**
 * Implements the Fetch API fetch() function
 * @param {string|Request} input - URL or Request object
 * @param {RequestInit} [init] - Request configuration
 * @returns {Promise<Response>} Response promise
 */
async function fetch(input, init = {}) {
    // Handle input parameter
    let url;
    if (typeof input === 'string') {
        url = input;
    } else if (input instanceof Request) {
        url = input.url;
        // Merge Request object properties with init
        init = {
            method: input.method,
            headers: input.headers,
            body: input.body,
            ...init
        };
    } else {
        throw new TypeError('First argument must be a URL string or Request object');
    }

    // Process init options
    const method = (init.method || 'GET').toUpperCase();
    const headers = init.headers ? Object.fromEntries(
        Object.entries(init.headers).map(([k, v]) => [k.toLowerCase(), String(v)])
    ) : null;
    const body = init.body ? String(init.body) : null;
    const timeout_ns = init.timeout ? BigInt(init.timeout) * 1000000n : null; // Convert ms to ns

    // Call Rust implementation
    try {
        return await do_fetch(method, url, headers, body, timeout_ns);
    } catch (error) {
        // Convert Rust errors to standard fetch errors
        if (error.message.includes('timeout')) {
            throw new TypeError('Network request failed: timeout');
        }
        if (error.message.includes('dns')) {
            throw new TypeError('Network request failed: DNS error');
        }
        throw new TypeError('Network request failed: ' + error.message);
    }
}

/**
 * Request class implementing the Web Fetch API Request interface
 */
class Request {
    #method;
    #url;
    #headers;
    #body;

    /**
     * @param {string|Request} input - URL or Request object 
     * @param {RequestInit} [init] - Request configuration
     */
    constructor(input, init = {}) {
        if (typeof input === 'string') {
            this.#url = input;
        } else if (input instanceof Request) {
            this.#url = input.url;
            this.#method = input.method;
            this.#headers = {...input.headers};
            this.#body = input.body;
        } else {
            throw new TypeError('First argument must be a URL string or Request object');
        }

        // Override with init properties
        this.#method = (init.method || this.#method || 'GET').toUpperCase();
        this.#headers = init.headers || this.#headers || {};
        this.#body = init.body !== undefined ? init.body : this.#body;
    }

    get method() { return this.#method; }
    get url() { return this.#url; }
    get headers() { return this.#headers; }
    get body() { return this.#body; }

    /**
     * Creates a copy of the request
     * @returns {Request}
     */
    clone() {
        return new Request(this.#url, {
            method: this.#method,
            headers: {...this.#headers},
            body: this.#body
        });
    }
}

export { fetch, Request, Headers };
