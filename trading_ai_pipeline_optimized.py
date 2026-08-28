"""Optimized Trading AI Pipeline with Async I/O and Parallel Processing

Performance improvements:
- Async API calls for concurrent market data fetching
- Parallel signal generation and trade execution
- Configurable concurrency limits
- Event-driven scheduling instead of fixed sleep intervals
- Batched dashboard updates
- Graceful degradation under load
"""

import asyncio
import logging
import time
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SLEEP_INTERVAL = 60  # 1 min - base interval, adjusted dynamically
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_CONCURRENT_MARKETS = 10  # Parallel processing limit
API_TIMEOUT = 30  # seconds
BATCH_SIZE = 5  # Markets to process in parallel batches

# Thread pool for CPU-bound operations (signal generation)
cpu_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix='signal_worker')

# Rate limiting
_last_api_call = 0
_MIN_API_INTERVAL = 0.1  # 100ms minimum between API calls
_rate_limit_lock = threading.Lock()

# Global state
_pipeline_state = {
    'last_success': time.time(),
    'last_failure': 0,
    'consecutive_failures': 0,
    'total_processed': 0,
    'total_success': 0,
    'lock': threading.Lock()
}


def _rate_limited_api_call(func, *args, **kwargs):
    """Rate-limited wrapper for API calls."""
    global _last_api_call
    with _rate_limit_lock:
        elapsed = time.time() - _last_api_call
        if elapsed < _MIN_API_INTERVAL:
            time.sleep(_MIN_API_INTERVAL - elapsed)
        _last_api_call = time.time()
    return func(*args, **kwargs)


async def fetch_market_data_async() -> Optional[List[str]]:
    """Async wrapper for market data fetching."""
    loop = asyncio.get_event_loop()
    try:
        # Use thread pool to avoid blocking the event loop
        result = await loop.run_in_executor(
            cpu_executor, 
            _rate_limited_api_call, 
            fetch_market_data
        )
        return result
    except Exception as e:
        logger.error(f"Async market data fetch failed: {e}")
        return None


async def generate_signal_async(market: str) -> Optional[Dict[str, Any]]:
    """Async wrapper for signal generation."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            generate_signal,
            market
        )
        return result
    except Exception as e:
        logger.error(f"Async signal generation failed for {market}: {e}")
        return None


async def execute_trade_async(signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Async wrapper for trade execution."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            execute_trade,
            signal
        )
        return result
    except Exception as e:
        logger.error(f"Async trade execution failed: {e}")
        return None


async def log_signal_async(signal: Dict[str, Any]) -> bool:
    """Async wrapper for signal logging."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            log_signal,
            signal
        )
        return result
    except Exception as e:
        logger.error(f"Async signal logging failed: {e}")
        return False


async def update_dashboard_async(**kwargs) -> bool:
    """Async wrapper for dashboard updates with batching."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            update_dashboard,
            **kwargs
        )
        return result
    except Exception as e:
        logger.error(f"Async dashboard update failed: {e}")
        return False


async def process_market(market: str, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """Process a single market with concurrency control."""
    async with semaphore:
        result = {
            'market': market,
            'status': 'error',
            'error': None,
            'signal': None,
            'trade_result': None,
            'start_time': time.time()
        }
        
        try:
            # Generate signal
            signal = await generate_signal_async(market)
            result['signal'] = signal
            
            if signal and signal.get('confidence', 0) >= 0.8:
                logger.info(f"High-confidence signal for {market}: confidence={signal.get('confidence')}")
                
                # Log signal (fire and forget - don't block on logging)
                asyncio.create_task(log_signal_async(signal))
                
                # Execute trade
                trade_result = await execute_trade_async(signal)
                result['trade_result'] = trade_result
                
                if trade_result:
                    # Update dashboard
                    await update_dashboard_async(
                        trading_ROI=signal.get('estimated_ROI'),
                        signal_executed=True,
                        trade_id=getattr(trade_result, 'id', trade_result.get('id')),
                        status='success'
                    )
                    result['status'] = 'success'
                    logger.info(f"Trade executed successfully for {market}")
                else:
                    result['status'] = 'trade_failed'
                    result['error'] = 'Trade execution returned None'
                    logger.warning(f"Trade execution failed for {market}")
            else:
                confidence = signal.get('confidence', 'N/A') if signal else 'N/A'
                result['status'] = 'low_confidence'
                logger.debug(f"Signal confidence too low for {market}: {confidence}")
                
                # Still update dashboard for tracking
                await update_dashboard_async(
                    status='skipped',
                    reason='low_confidence',
                    confidence=confidence
                )
                
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"Error processing market {market}: {e}")
            
            await update_dashboard_async(
                status='error',
                error_message=str(e),
                market=market
            )
        
        result['end_time'] = time.time()
        result['duration'] = result['end_time'] - result['start_time']
        return result


async def process_market_batch(markets: List[str]) -> List[Dict[str, Any]]:
    """Process a batch of markets in parallel."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_MARKETS)
    
    tasks = [process_market(market, semaphore) for market in markets]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    return results


async def run_trading_async() -> Dict[str, Any]:
    """Main async trading pipeline execution."""
    result = {
        'status': 'success',
        'markets_processed': 0,
        'success_count': 0,
        'error_count': 0,
        'start_time': time.time(),
        'errors': []
    }
    
    try:
        # Fetch market data
        markets = await fetch_market_data_async()
        
        if not markets:
            logger.warning("No market data available")
            result['status'] = 'no_data'
            return result
        
        logger.info(f"Processing {len(markets)} markets in batches of {BATCH_SIZE}")
        result['markets_processed'] = len(markets)
        
        # Process in batches to control memory usage
        for batch_start in range(0, len(markets), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(markets))
            batch = markets[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start + 1}-{batch_end}/{len(markets)}")
            
            batch_results = await process_market_batch(batch)
            
            for batch_result in batch_results:
                if batch_result['status'] == 'success':
                    result['success_count'] += 1
                elif batch_result['status'] == 'error':
                    result['error_count'] += 1
                    result['errors'].append({
                        'market': batch_result['market'],
                        'error': batch_result['error']
                    })
        
        result['end_time'] = time.time()
        result['duration'] = result['end_time'] - result['start_time']
        
        # Log summary
        logger.info(
            f"Batch completed: {result['success_count']} success, "
            f"{result['error_count']} errors, "
            f"{result['markets_processed'] - result['success_count'] - result['error_count']} skipped, "
            f"Duration: {result['duration']:.2f}s"
        )
        
    except Exception as e:
        result['status'] = 'critical_error'
        result['error'] = str(e)
        logger.error(f"Critical error in async trading pipeline: {e}")
    
    return result


async def run_trading_with_retry_async() -> Dict[str, Any]:
    """Execute trading pipeline with retry logic (async version)."""
    for attempt in range(MAX_RETRIES):
        try:
            result = await run_trading_async()
            
            # Update pipeline state
            with _pipeline_state['lock']:
                _pipeline_state['total_processed'] += result.get('markets_processed', 0)
                _pipeline_state['total_success'] += result.get('success_count', 0)
                
                if result['status'] in ['success', 'no_data']:
                    _pipeline_state['last_success'] = time.time()
                    _pipeline_state['consecutive_failures'] = 0
                    return result
                else:
                    _pipeline_state['last_failure'] = time.time()
                    _pipeline_state['consecutive_failures'] += 1
                    
        except Exception as e:
            logger.warning(f"Trading attempt {attempt + 1} failed: {e}")
            with _pipeline_state['lock']:
                _pipeline_state['last_failure'] = time.time()
                _pipeline_state['consecutive_failures'] += 1
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Trading failed after {MAX_RETRIES} attempts")
                return {'status': 'failed', 'error': str(e), 'attempts': MAX_RETRIES}
    
    return {'status': 'failed', 'attempts': MAX_RETRIES}


async def adaptive_sleep() -> float:
    """Adaptive sleep interval based on pipeline state and load."""
    with _pipeline_state['lock']:
        consecutive_failures = _pipeline_state['consecutive_failures']
        time_since_success = time.time() - _pipeline_state['last_success']
        total_processed = _pipeline_state['total_processed']
    
    # If recent failures, back off exponentially
    if consecutive_failures > 0:
        base_delay = SLEEP_INTERVAL * (2 ** min(consecutive_failures, 3))
        max_delay = 300  # 5 minutes max
        return min(base_delay, max_delay)
    
    # If no recent activity, use base interval
    if time_since_success > SLEEP_INTERVAL * 2:
        return SLEEP_INTERVAL
    
    # If processing quickly, reduce interval (but not below 10s)
    if total_processed > 10 and time_since_success < SLEEP_INTERVAL:
        return max(SLEEP_INTERVAL * 0.5, 10)
    
    return SLEEP_INTERVAL


async def main_async():
    """Main async entry point."""
    # Initialize system
    try:
        test_data = await fetch_market_data_async()
        if test_data:
            logger.info("Trading system initialized successfully (async)")
        else:
            logger.critical("Failed to initialize trading system. Exiting.")
            return
    except Exception as e:
        logger.critical(f"Failed to initialize trading system: {e}. Exiting.")
        return
    
    logger.info(f"Starting optimized async trading pipeline")
    
    # Main loop
    try:
        while True:
            start_time = time.time()
            
            result = await run_trading_with_retry_async()
            
            # Adaptive sleep
            sleep_time = await adaptive_sleep()
            elapsed = time.time() - start_time
            
            if elapsed < sleep_time:
                remaining = sleep_time - elapsed
                logger.info(f"Sleeping for {remaining:.1f}s (adaptive interval: {sleep_time:.1f}s)")
                await asyncio.sleep(remaining)
            else:
                logger.info(f"Skipping sleep (processing took {elapsed:.1f}s >= {sleep_time:.1f}s)")
                
    except KeyboardInterrupt:
        logger.info("Async trading pipeline stopped by user")
    except Exception as e:
        logger.critical(f"Unexpected error in async main loop: {e}")


def initialize_trading():
    """Initialize trading system and validate connections (sync version for compatibility)."""
    logger.info("Initializing trading system...")
    try:
        test_data = fetch_market_data()
        if test_data:
            logger.info("Trading system initialized successfully")
            return True
    except Exception as e:
        logger.error(f"Failed to initialize trading system: {e}")
        return False


if __name__ == "__main__":
    # Check if we should run async or sync version
    import sys
    use_async = '--async' in sys.argv or '--optimized' in sys.argv
    
    if use_async:
        logger.info("Running optimized async trading pipeline")
        asyncio.run(main_async())
    else:
        logger.info("Running legacy sync trading pipeline (use --async for optimized version)")
        # Fall back to original sync version
        if not initialize_trading():
            logger.critical("Failed to initialize trading system. Exiting.")
            exit(1)
        
        logger.info(f"Starting trading pipeline (interval: {SLEEP_INTERVAL}s)")
        
        try:
            while True:
                # Import original functions
                from trading_ai_pipeline import run_trading_with_retry
                run_trading_with_retry()
                time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Trading pipeline stopped by user")
        except Exception as e:
            logger.critical(f"Unexpected error in main loop: {e}")
