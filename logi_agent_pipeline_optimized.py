"""Optimized Logistics Agent Pipeline with Async I/O and Parallel Processing

Performance improvements:
- Async API calls for concurrent order/vehicle/carrier fetching
- Parallel order processing with configurable concurrency
- Batched dashboard updates
- Adaptive sleep intervals based on workload
- Rate limiting to prevent API overload
- Error resilience with retry logic
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
SLEEP_INTERVAL = 300  # 5 min - base interval, adjusted dynamically
MAX_CONCURRENT_ORDERS = 20  # Parallel processing limit
API_TIMEOUT = 30  # seconds
BATCH_SIZE = 10  # Orders to process in parallel batches
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Thread pool for CPU-bound operations
cpu_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix='logi_worker')

# Rate limiting
_last_api_calls = {
    'fetch_orders': 0,
    'fetch_vehicles': 0,
    'fetch_carriers': 0
}
_MIN_API_INTERVAL = 0.05  # 50ms minimum between API calls per endpoint
_rate_limit_locks = {
    'fetch_orders': threading.Lock(),
    'fetch_vehicles': threading.Lock(),
    'fetch_carriers': threading.Lock()
}

# Global state for adaptive scheduling
_pipeline_state = {
    'last_processing_time': 0,
    'orders_processed': 0,
    'last_success': time.time(),
    'consecutive_failures': 0,
    'lock': threading.Lock()
}


def _rate_limited_api_call(endpoint: str, func, *args, **kwargs):
    """Rate-limited wrapper for API calls."""
    lock = _rate_limit_locks.get(endpoint, threading.Lock())
    with lock:
        last_call = _last_api_calls.get(endpoint, 0)
        elapsed = time.time() - last_call
        if elapsed < _MIN_API_INTERVAL:
            time.sleep(_MIN_API_INTERVAL - elapsed)
        _last_api_calls[endpoint] = time.time()
    return func(*args, **kwargs)


async def fetch_orders_async(status: str = 'new') -> Optional[List[Dict[str, Any]]]:
    """Async wrapper for fetching orders."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            _rate_limited_api_call,
            'fetch_orders',
            fetch_orders,
            status
        )
        return result
    except Exception as e:
        logger.error(f"Async fetch_orders failed: {e}")
        return None


async def fetch_vehicles_async() -> Optional[List[Dict[str, Any]]]:
    """Async wrapper for fetching vehicles."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            _rate_limited_api_call,
            'fetch_vehicles',
            fetch_vehicles
        )
        return result
    except Exception as e:
        logger.error(f"Async fetch_vehicles failed: {e}")
        return None


async def fetch_carriers_async() -> Optional[List[Dict[str, Any]]]:
    """Async wrapper for fetching carriers."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            _rate_limited_api_call,
            'fetch_carriers',
            fetch_carriers
        )
        return result
    except Exception as e:
        logger.error(f"Async fetch_carriers failed: {e}")
        return None


async def assign_vehicle_async(order: Dict[str, Any], vehicles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Async wrapper for vehicle assignment."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            assign_vehicle,
            order,
            vehicles
        )
        return result
    except Exception as e:
        logger.error(f"Async assign_vehicle failed: {e}")
        return None


async def assign_carrier_async(order: Dict[str, Any], carriers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Async wrapper for carrier assignment."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            assign_carrier,
            order,
            carriers
        )
        return result
    except Exception as e:
        logger.error(f"Async assign_carrier failed: {e}")
        return None


async def calculate_margin_async(order: Dict[str, Any], vehicle: Dict[str, Any], carrier: Dict[str, Any]) -> Optional[float]:
    """Async wrapper for margin calculation."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            calculate_margin,
            order,
            vehicle,
            carrier
        )
        return result
    except Exception as e:
        logger.error(f"Async calculate_margin failed: {e}")
        return None


async def check_backhaul_async(order: Dict[str, Any]) -> Optional[bool]:
    """Async wrapper for backhaul check."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            check_backhaul,
            order
        )
        return result
    except Exception as e:
        logger.error(f"Async check_backhaul failed: {e}")
        return None


async def check_consolidation_async(order: Dict[str, Any]) -> Optional[bool]:
    """Async wrapper for consolidation check."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            check_consolidation,
            order
        )
        return result
    except Exception as e:
        logger.error(f"Async check_consolidation failed: {e}")
        return None


async def update_dashboard_async(order_id: str, vehicle: Dict[str, Any], carrier: Dict[str, Any], 
                                margin: float, backhaul: bool, consolidation: bool) -> bool:
    """Async wrapper for dashboard updates."""
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            cpu_executor,
            update_dashboard,
            order_id,
            vehicle,
            carrier,
            margin,
            backhaul,
            consolidation
        )
        return result
    except Exception as e:
        logger.error(f"Async update_dashboard failed: {e}")
        return False


async def process_order(order: Dict[str, Any], vehicles: List[Dict[str, Any]], 
                       carriers: List[Dict[str, Any]], semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """Process a single order with concurrency control."""
    async with semaphore:
        result = {
            'order_id': order.get('id'),
            'status': 'error',
            'error': None,
            'vehicle': None,
            'carrier': None,
            'margin': None,
            'backhaul': None,
            'consolidation': None,
            'start_time': time.time()
        }
        
        try:
            # Parallel assignment (vehicle and carrier can be done concurrently)
            vehicle_task = asyncio.create_task(assign_vehicle_async(order, vehicles))
            carrier_task = asyncio.create_task(assign_carrier_async(order, carriers))
            
            vehicle = await vehicle_task
            carrier = await carrier_task
            
            result['vehicle'] = vehicle
            result['carrier'] = carrier
            
            if vehicle and carrier:
                # Calculate margin (depends on both vehicle and carrier)
                margin = await calculate_margin_async(order, vehicle, carrier)
                result['margin'] = margin
                
                # Parallel checks (backhaul and consolidation are independent)
                backhaul_task = asyncio.create_task(check_backhaul_async(order))
                consolidation_task = asyncio.create_task(check_consolidation_async(order))
                
                backhaul = await backhaul_task
                consolidation = await consolidation_task
                
                result['backhaul'] = backhaul
                result['consolidation'] = consolidation
                
                # Update dashboard
                if vehicle and carrier:
                    success = await update_dashboard_async(
                        order.get('id'),
                        vehicle,
                        carrier,
                        margin or 0.0,
                        backhaul or False,
                        consolidation or False
                    )
                    
                    if success:
                        result['status'] = 'success'
                    else:
                        result['status'] = 'dashboard_update_failed'
                        result['error'] = 'Dashboard update failed'
                else:
                    result['status'] = 'missing_data'
                    result['error'] = 'Vehicle or carrier assignment failed'
            else:
                result['status'] = 'assignment_failed'
                result['error'] = 'Vehicle or carrier assignment returned None'
                
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"Error processing order {order.get('id')}: {e}")
        
        result['end_time'] = time.time()
        result['duration'] = result['end_time'] - result['start_time']
        return result


async def process_order_batch(orders: List[Dict[str, Any]], vehicles: List[Dict[str, Any]], 
                            carriers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process a batch of orders in parallel."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_ORDERS)
    
    tasks = [
        process_order(order, vehicles, carriers, semaphore) 
        for order in orders
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    return results


async def run_pipeline_async() -> Dict[str, Any]:
    """Main async pipeline execution."""
    result = {
        'status': 'success',
        'orders_processed': 0,
        'success_count': 0,
        'error_count': 0,
        'start_time': time.time(),
        'errors': []
    }
    
    try:
        # Fetch all data in parallel
        logger.info("Fetching orders, vehicles, and carriers in parallel...")
        
        orders_task = asyncio.create_task(fetch_orders_async(status='new'))
        vehicles_task = asyncio.create_task(fetch_vehicles_async())
        carriers_task = asyncio.create_task(fetch_carriers_async())
        
        orders = await orders_task
        vehicles = await vehicles_task
        carriers = await carriers_task
        
        if not orders:
            logger.info("No new orders available")
            result['status'] = 'no_orders'
            return result
        
        if not vehicles:
            logger.warning("No vehicles available")
            result['status'] = 'no_vehicles'
            return result
        
        if not carriers:
            logger.warning("No carriers available")
            result['status'] = 'no_carriers'
            return result
        
        logger.info(f"Processing {len(orders)} orders in batches of {BATCH_SIZE}")
        result['orders_processed'] = len(orders)
        
        # Process in batches to control memory usage
        for batch_start in range(0, len(orders), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(orders))
            batch = orders[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start + 1}-{batch_end}/{len(orders)}")
            
            batch_results = await process_order_batch(batch, vehicles, carriers)
            
            for batch_result in batch_results:
                if batch_result['status'] == 'success':
                    result['success_count'] += 1
                elif batch_result['status'] == 'error':
                    result['error_count'] += 1
                    result['errors'].append({
                        'order_id': batch_result['order_id'],
                        'error': batch_result['error']
                    })
        
        result['end_time'] = time.time()
        result['duration'] = result['end_time'] - result['start_time']
        
        # Log summary
        logger.info(
            f"Pipeline completed: {result['success_count']} success, "
            f"{result['error_count']} errors, "
            f"Duration: {result['duration']:.2f}s"
        )
        
        # Update pipeline state
        with _pipeline_state['lock']:
            _pipeline_state['orders_processed'] += result['orders_processed']
            _pipeline_state['last_success'] = time.time()
            _pipeline_state['consecutive_failures'] = 0
            _pipeline_state['last_processing_time'] = result['duration']
        
    except Exception as e:
        result['status'] = 'critical_error'
        result['error'] = str(e)
        logger.error(f"Critical error in async pipeline: {e}")
        
        with _pipeline_state['lock']:
            _pipeline_state['consecutive_failures'] += 1
    
    return result


async def run_pipeline_with_retry_async() -> Dict[str, Any]:
    """Execute pipeline with retry logic (async version)."""
    for attempt in range(MAX_RETRIES):
        try:
            result = await run_pipeline_async()
            
            if result['status'] in ['success', 'no_orders', 'no_vehicles', 'no_carriers']:
                return result
                
        except Exception as e:
            logger.warning(f"Pipeline attempt {attempt + 1} failed: {e}")
            
            with _pipeline_state['lock']:
                _pipeline_state['consecutive_failures'] += 1
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Pipeline failed after {MAX_RETRIES} attempts")
                return {'status': 'failed', 'error': str(e), 'attempts': MAX_RETRIES}
    
    return {'status': 'failed', 'attempts': MAX_RETRIES}


async def adaptive_sleep() -> float:
    """Adaptive sleep interval based on pipeline state and load."""
    with _pipeline_state['lock']:
        consecutive_failures = _pipeline_state['consecutive_failures']
        time_since_success = time.time() - _pipeline_state['last_success']
        avg_processing_time = _pipeline_state.get('last_processing_time', SLEEP_INTERVAL)
        orders_processed = _pipeline_state['orders_processed']
    
    # If recent failures, back off exponentially
    if consecutive_failures > 0:
        base_delay = SLEEP_INTERVAL * (2 ** min(consecutive_failures, 3))
        max_delay = 600  # 10 minutes max
        return min(base_delay, max_delay)
    
    # If processing is fast, reduce interval (but not below 60s)
    if avg_processing_time < SLEEP_INTERVAL * 0.3 and orders_processed > 0:
        return max(SLEEP_INTERVAL * 0.5, 60)
    
    # If no recent activity, use base interval
    if time_since_success > SLEEP_INTERVAL * 2:
        return SLEEP_INTERVAL
    
    return SLEEP_INTERVAL


async def main_async():
    """Main async entry point."""
    logger.info("Starting optimized async logistics agent pipeline")
    
    # Main loop
    try:
        while True:
            start_time = time.time()
            
            result = await run_pipeline_with_retry_async()
            
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
        logger.info("Async logistics pipeline stopped by user")
    except Exception as e:
        logger.critical(f"Unexpected error in async main loop: {e}")


if __name__ == "__main__":
    import sys
    use_async = '--async' in sys.argv or '--optimized' in sys.argv
    
    if use_async:
        logger.info("Running optimized async logistics pipeline")
        asyncio.run(main_async())
    else:
        logger.info("Running legacy sync logistics pipeline (use --async for optimized version)")
        # Fall back to original sync version
        while True:
            from logi_agent_pipeline import run_pipeline
            run_pipeline()
            time.sleep(SLEEP_INTERVAL)
