/**
 * Meadow MSSQL Retry + Error Classification
 *
 * Shared helper used by both the connection provider (Meadow-Connection-MSSQL)
 * and the schema provider (Meadow-Schema-MSSQL) to:
 *
 *   1. Classify MSSQL driver errors into a small set of well-known failure
 *      modes, so logs make it obvious WHY an operation failed (network,
 *      lock contention, pool degradation, etc.).
 *   2. Retry transient operations with exponential backoff, logging each
 *      attempt in a way that makes the failure mode sequence readable.
 *   3. Optionally recycle the connection pool on failures that suggest the
 *      pooled connection is in a bad state (timeouts, network drops).
 *
 * Each failure mode is described with a human-readable string so a
 * developer reading a sync log can tell at a glance whether the problem
 * is "slow network", "server-side DDL lock", or "pool went bad after
 * timeout" — these are very different things and each has a different
 * resolution path.
 *
 * @license MIT
 * @author Steven Velozo <steven@velozo.com>
 */

'use strict';

/**
 * Default retry options.  Callers may override any of these per-operation.
 *
 *   MaxAttempts        — total attempts including the first.  5 means
 *                        "first try + up to 4 retries".
 *   InitialDelayMs     — wait before the second attempt.  Subsequent
 *                        retries use exponential backoff from here.
 *   MaxDelayMs         — cap the backoff so we don't wait absurdly long.
 *   BackoffFactor      — multiplier between retries (2 = double each time).
 *   PoolRecycleModes   — failure modes that trigger pool recycling before
 *                        the next attempt.  Kept explicit so callers can
 *                        narrow or widen the set without reading code.
 */
const DEFAULT_RETRY_OPTIONS =
{
	MaxAttempts: 5,
	InitialDelayMs: 2000,
	MaxDelayMs: 30000,
	BackoffFactor: 2,
	PoolRecycleModes: ['NetworkError', 'RequestTimeout', 'PoolDegraded'],
	// Hard wall-clock ceiling per attempt.  0 disables it (legacy behavior).
	// The mssql driver's own requestTimeout normally fires first; this is the
	// backstop for the case where the driver's callback never arrives at all
	// (a wedged socket, a pool acquire that never resolves), which otherwise
	// hangs the whole operation — and any sync walking it — forever.
	OperationTimeoutMs: 0
};

/**
 * Error-mode strings.  Exported so callers can reference them in
 * configuration (e.g. extending PoolRecycleModes) without stringly-typing.
 */
const ERROR_MODES =
{
	AlreadyExists:  'AlreadyExists',
	NetworkError:   'NetworkError',
	RequestTimeout: 'RequestTimeout',
	PoolDegraded:   'PoolDegraded',
	ServerError:    'ServerError',
	Unknown:        'Unknown'
};

/**
 * Extract the best available diagnostic string from a node-mssql error.
 * The driver stashes the real server message in different places depending
 * on the failure path — probe all of them and concatenate what we find.
 *
 * @param {Error} pError
 * @return {string}
 */
function extractErrorMessage(pError)
{
	if (!pError) return '';

	let tmpTop = pError.message || '';

	let tmpInner = '';
	if (pError.originalError)
	{
		if (pError.originalError.info && pError.originalError.info.message)
		{
			tmpInner = pError.originalError.info.message;
		}
		else if (pError.originalError.message)
		{
			tmpInner = pError.originalError.message;
		}
	}

	if (tmpInner && tmpInner !== tmpTop)
	{
		return `${tmpTop} — ${tmpInner}`;
	}
	return tmpTop;
}

/**
 * Classify a node-mssql error into one of the well-known failure modes.
 *
 * @param {Error}  pError       — the error from the mssql driver
 * @param {number} pElapsedMs   — how long the failed attempt took
 * @param {Object} [pPriorInfo] — { lastFailureMode, lastFailureElapsed }
 *                                from the most recent prior attempt; used
 *                                to detect the fast-fail-after-timeout
 *                                pattern that signals pool degradation.
 *
 * @return {{
 *   mode: string,
 *   description: string,
 *   isRetryable: boolean,
 *   recommendPoolRecycle: boolean
 * }}
 */
function classifyError(pError, pElapsedMs, pPriorInfo)
{
	let tmpCode = (pError && pError.code) || '';
	let tmpMsg = extractErrorMessage(pError);
	let tmpMsgLower = tmpMsg.toLowerCase();

	// Benign case — target already exists.  Callers should treat as success.
	if (tmpMsgLower.indexOf('there is already an object named') >= 0)
	{
		return {
			mode: ERROR_MODES.AlreadyExists,
			description: 'target already exists (benign on re-deploy)',
			isRetryable: false,
			recommendPoolRecycle: false
		};
	}

	// Server-level error (syntax, permissions, constraint violations).
	// These are deterministic — retrying won't help.  Checked early so we
	// don't misclassify a server message as a timeout just because it
	// contains the word "timeout" somewhere coincidentally.
	if (tmpCode === 'EREQUEST' && tmpMsgLower.indexOf('timeout') < 0)
	{
		return {
			mode: ERROR_MODES.ServerError,
			description: `server-side SQL error: ${tmpMsg}`,
			isRetryable: false,
			recommendPoolRecycle: false
		};
	}

	// Pool-degraded heuristic: a fast failure (<500ms) right after a
	// prior slow failure (>5s) almost always means the pooled connection
	// is in a bad state — the server was slow, the socket went stale,
	// and the next query on the same pool died fast.  Check this BEFORE
	// the generic NetworkError/RequestTimeout branches so the specific
	// mode wins in the log.
	if (pElapsedMs < 500 &&
		pPriorInfo &&
		pPriorInfo.lastFailureMode === ERROR_MODES.RequestTimeout &&
		pPriorInfo.lastFailureElapsed > 5000)
	{
		return {
			mode: ERROR_MODES.PoolDegraded,
			description: `query failed in ${pElapsedMs}ms after prior ${(pPriorInfo.lastFailureElapsed / 1000).toFixed(1)}s timeout — connection pool appears degraded (stale/bad connection)`,
			isRetryable: true,
			recommendPoolRecycle: true
		};
	}

	// OS-level socket timeout (ETIMEDOUT) — the kernel gave up trying to
	// complete the TCP operation.  Treat as a network failure.
	if (tmpCode === 'ETIMEDOUT')
	{
		return {
			mode: ERROR_MODES.NetworkError,
			description: `network-level timeout (${tmpCode}) — socket didn't complete in OS-level timeout: ${tmpMsg}`,
			isRetryable: true,
			recommendPoolRecycle: true
		};
	}

	// Network-level failures — server unreachable, DNS, socket dropped.
	// These all indicate the problem is below the SQL layer.
	if (tmpCode === 'ECONNREFUSED' ||
		tmpCode === 'ECONNRESET' ||
		tmpCode === 'ENETUNREACH' ||
		tmpCode === 'EHOSTUNREACH' ||
		tmpCode === 'ENOTFOUND' ||
		tmpCode === 'EAI_AGAIN' ||
		tmpCode === 'ESOCKET')
	{
		return {
			mode: ERROR_MODES.NetworkError,
			description: `network-level failure (${tmpCode}) — server unreachable or connection dropped: ${tmpMsg}`,
			isRetryable: true,
			recommendPoolRecycle: true
		};
	}

	// Request timeout — the query reached the server but didn't respond in
	// time.  On DDL operations this almost always means another transaction
	// is holding an exclusive schema lock.  On data queries it could mean a
	// long-running query is blocking, or the server is overloaded.  Note
	// the code is `ETIMEOUT` (no D), which is node-mssql / tedious's own
	// request-timeout code, distinct from the kernel-level ETIMEDOUT above.
	if (tmpCode === 'ETIMEOUT' ||
		tmpMsgLower.indexOf('operation timed out') >= 0 ||
		tmpMsgLower.indexOf('request timeout') >= 0 ||
		tmpMsgLower.indexOf('timed out for an unknown reason') >= 0)
	{
		return {
			mode: ERROR_MODES.RequestTimeout,
			description: `request timed out after ${(pElapsedMs / 1000).toFixed(1)}s — likely DDL lock contention on the server, a long-running blocking query, or a very slow server response`,
			isRetryable: true,
			recommendPoolRecycle: true
		};
	}

	// Everything else — retry cautiously (it might be transient) but don't
	// recycle the pool since we don't know what kind of failure it is.
	return {
		mode: ERROR_MODES.Unknown,
		description: `unclassified error (${tmpCode || 'no code'}): ${tmpMsg}`,
		isRetryable: true,
		recommendPoolRecycle: false
	};
}

/**
 * Run an operation with retry, exponential backoff, error classification,
 * and optional pool recycling.  Callback-style to match the rest of the
 * Meadow codebase.
 *
 * The operation function is called with a single `fDone(err, result)`
 * callback.  `err` may be null on success.  Anything truthy is classified
 * and either retried (if the mode is retryable) or propagated.
 *
 * Log output is structured so a reader can follow the decision tree:
 *
 *   [info]  Meadow-MSSQL CREATE TABLE Sample: attempt 1/5 starting...
 *   [warn]  Meadow-MSSQL CREATE TABLE Sample: attempt 1/5 failed after 30012ms — request timed out ... (likely DDL lock contention ...)
 *   [info]  Meadow-MSSQL CREATE TABLE Sample: recycling connection pool before retry (mode: RequestTimeout)
 *   [info]  Meadow-MSSQL CREATE TABLE Sample: retrying in 2s (attempt 2/5)
 *   [info]  Meadow-MSSQL CREATE TABLE Sample: attempt 2/5 starting...
 *   [info]  Meadow-MSSQL CREATE TABLE Sample: succeeded on attempt 2 (elapsed 3542ms)
 *
 * @param {Object}   pLog       — a fable logger (has .info, .warn, .error)
 * @param {Object}   pOptions   — { OperationName, MaxAttempts, InitialDelayMs, MaxDelayMs, BackoffFactor, PoolRecycleModes, OnRecyclePool, SuccessModes }
 * @param {Function} fOperation — (fDone) => ... where fDone(err, result)
 * @param {Function} fCallback  — (err, result) => ...
 */
function runWithRetry(pLog, pOptions, fOperation, fCallback)
{
	let tmpOptions = Object.assign({}, DEFAULT_RETRY_OPTIONS, pOptions || {});
	let tmpOpName = tmpOptions.OperationName || 'Meadow-MSSQL operation';
	let tmpPriorInfo = null;
	let tmpAttempt = 0;

	// SuccessModes lets callers treat certain failure modes as success
	// (e.g. AlreadyExists for CREATE TABLE — not really an error).
	let tmpSuccessModes = tmpOptions.SuccessModes || [ERROR_MODES.AlreadyExists];

	let fTryOnce = () =>
	{
		tmpAttempt++;
		let tmpStartMs = Date.now();
		let tmpSettled = false;
		let tmpTimeoutHandle = null;

		pLog.info(`${tmpOpName}: attempt ${tmpAttempt}/${tmpOptions.MaxAttempts} starting...`);

		let fHandleOutcome = (pError, pResult) =>
		{
			// Exactly one outcome per attempt: whichever of the operation
			// callback or the hard-timeout guard fires first wins; the other is
			// a no-op.
			if (tmpSettled)
			{
				return;
			}
			tmpSettled = true;
			if (tmpTimeoutHandle)
			{
				clearTimeout(tmpTimeoutHandle);
				tmpTimeoutHandle = null;
			}

			let tmpElapsedMs = Date.now() - tmpStartMs;

			if (!pError)
			{
				if (tmpAttempt > 1)
				{
					pLog.info(`${tmpOpName}: succeeded on attempt ${tmpAttempt} (elapsed ${tmpElapsedMs}ms)`);
				}
				return fCallback(null, pResult);
			}

			let tmpInfo = classifyError(pError, tmpElapsedMs, tmpPriorInfo);

			// Treat success-mode failures as success.
			if (tmpSuccessModes.indexOf(tmpInfo.mode) >= 0)
			{
				pLog.info(`${tmpOpName}: ${tmpInfo.description} — treating as success`);
				return fCallback(null, pResult);
			}

			pLog.warn(`${tmpOpName}: attempt ${tmpAttempt}/${tmpOptions.MaxAttempts} failed after ${tmpElapsedMs}ms [${tmpInfo.mode}] — ${tmpInfo.description}`);

			tmpPriorInfo = { lastFailureMode: tmpInfo.mode, lastFailureElapsed: tmpElapsedMs };

			// Terminal: non-retryable or exhausted attempts.
			if (!tmpInfo.isRetryable)
			{
				pLog.error(`${tmpOpName}: giving up — ${tmpInfo.mode} is not retryable`);
				return fCallback(pError);
			}
			if (tmpAttempt >= tmpOptions.MaxAttempts)
			{
				pLog.error(`${tmpOpName}: giving up — exhausted ${tmpOptions.MaxAttempts} attempts; last failure mode was ${tmpInfo.mode}`);
				return fCallback(pError);
			}

			// Compute next delay with exponential backoff, capped at MaxDelayMs.
			let tmpDelayMs = Math.min(
				tmpOptions.InitialDelayMs * Math.pow(tmpOptions.BackoffFactor, tmpAttempt - 1),
				tmpOptions.MaxDelayMs);

			let fScheduleRetry = () =>
			{
				pLog.info(`${tmpOpName}: retrying in ${(tmpDelayMs / 1000).toFixed(1)}s (attempt ${tmpAttempt + 1}/${tmpOptions.MaxAttempts})`);
				setTimeout(fTryOnce, tmpDelayMs);
			};

			// Recycle the pool if the failure mode suggests it AND the caller
			// provided a recycling hook.  Pool recycling is async too — wait
			// for it to finish before scheduling the retry.
			let tmpShouldRecycle = tmpOptions.PoolRecycleModes.indexOf(tmpInfo.mode) >= 0
				&& tmpInfo.recommendPoolRecycle
				&& typeof (tmpOptions.OnRecyclePool) === 'function';

			if (tmpShouldRecycle)
			{
				pLog.info(`${tmpOpName}: recycling connection pool before retry (mode: ${tmpInfo.mode})`);
				tmpOptions.OnRecyclePool((pRecycleError) =>
				{
					if (pRecycleError)
					{
						pLog.warn(`${tmpOpName}: pool recycle failed — ${pRecycleError.message || pRecycleError}; retrying anyway`);
					}
					fScheduleRetry();
				});
				return;
			}

			fScheduleRetry();
		};

		// Arm the hard wall-clock guard for this attempt.  If the operation's
		// callback never arrives (driver requestTimeout didn't fire — wedged
		// socket / stuck pool acquire), synthesize an ETIMEOUT so the normal
		// classify -> recycle-pool -> retry path runs instead of hanging.  A late
		// real callback is ignored via tmpSettled inside fHandleOutcome.
		if (tmpOptions.OperationTimeoutMs > 0)
		{
			tmpTimeoutHandle = setTimeout(() =>
			{
				let tmpTimeoutError = new Error(`operation exceeded hard timeout of ${(tmpOptions.OperationTimeoutMs / 1000).toFixed(0)}s with no driver response`);
				tmpTimeoutError.code = 'ETIMEOUT';
				fHandleOutcome(tmpTimeoutError);
			}, tmpOptions.OperationTimeoutMs);
			// The guard timer must not, by itself, keep the process alive.
			if (tmpTimeoutHandle && typeof(tmpTimeoutHandle.unref) === 'function')
			{
				tmpTimeoutHandle.unref();
			}
		}

		fOperation(fHandleOutcome);
	};

	fTryOnce();
}

module.exports = {
	classifyError: classifyError,
	runWithRetry: runWithRetry,
	extractErrorMessage: extractErrorMessage,
	DEFAULT_RETRY_OPTIONS: DEFAULT_RETRY_OPTIONS,
	ERROR_MODES: ERROR_MODES
};
