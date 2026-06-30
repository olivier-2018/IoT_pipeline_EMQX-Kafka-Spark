# Main Orchestrator for All Data Generators
# Runs all 5 generators in parallel threads, throttles to target throughput

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from config import GENERATION_CONFIG, LOG_FORMAT
from weather_generator import WeatherGenerator
from orders_generator import OrdersGenerator
from logistics_generator import LogisticsGenerator
from inventory_generator import InventoryGenerator
from user_events_generator import UserEventsGenerator

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class GeneratorOrchestrator:
    def __init__(self):
        self.generators = {
            "weather": WeatherGenerator(),
            "orders": OrdersGenerator(),
            "logistics": LogisticsGenerator(),
            "inventory": InventoryGenerator(),
            "users": UserEventsGenerator(),
        }
        self.config = GENERATION_CONFIG
        self.running = False
        self.total_published = 0
        self.start_time = None

    def connect_all(self):
        """Connect all generators to MQTT broker"""
        logger.info("=== Connecting All Generators ===")
        for name, gen in self.generators.items():
            try:
                logger.info(f"Connecting {name} generator...")
                gen.connect()
                time.sleep(0.5)  # Stagger connections
            except Exception as e:
                logger.error(f"Failed to connect {name}: {e}")
                raise

        logger.info("✓ All generators connected!")
        time.sleep(2)  # Wait for MQTT connections to stabilize

    def disconnect_all(self):
        """Disconnect all generators"""
        logger.info("=== Disconnecting All Generators ===")
        for name, gen in self.generators.items():
            try:
                gen.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting {name}: {e}")

    def run_batch_cycle(self):
        """Run one batch cycle across all generators"""
        cycle_start = time.time()
        cycle_published = 0

        # Run generators in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(gen.run_batch): name
                for name, gen in self.generators.items()
            }

            for future in as_completed(futures):
                gen_name = futures[future]
                try:
                    published = future.result()
                    cycle_published += published
                except Exception as e:
                    logger.error(f"Error in {gen_name}: {e}")

        cycle_time = time.time() - cycle_start
        return cycle_published, cycle_time

    def run(self, duration_minutes: int = None):
        """Run generators for specified duration"""
        if duration_minutes is None:
            duration_minutes = self.config["duration_minutes"]

        self.running = True
        self.start_time = datetime.now()
        end_time = self.start_time + timedelta(minutes=duration_minutes)
        frequency_seconds = self.config["frequency_seconds"]

        logger.info(f"=== Starting Generator Orchestrator ===")
        logger.info(f"Duration: {duration_minutes} minutes")
        logger.info(f"Batch Frequency: {frequency_seconds} seconds")
        logger.info(f"Target Throughput: {self.config['batch_size_target']} msg/sec")
        logger.info("")

        try:
            self.connect_all()

            batch_count = 0
            while datetime.now() < end_time and self.running:
                batch_count += 1
                cycle_start = datetime.now()

                # Run one batch cycle
                cycle_published, cycle_time = self.run_batch_cycle()
                self.total_published += cycle_published

                # Calculate elapsed time and throttle if needed
                elapsed = (datetime.now() - self.start_time).total_seconds()
                throughput = self.total_published / elapsed if elapsed > 0 else 0

                logger.info(
                    f"[Batch {batch_count}] Published: {cycle_published} | "
                    f"Total: {self.total_published} | "
                    f"Throughput: {throughput:.1f} msg/sec | "
                    f"Cycle Time: {cycle_time:.2f}s"
                )

                # Throttle: sleep until next batch is due
                sleep_time = frequency_seconds - cycle_time
                if sleep_time > 0:
                    logger.debug(f"Throttling for {sleep_time:.2f}s")
                    time.sleep(sleep_time)

                # Check if time remaining
                remaining = (end_time - datetime.now()).total_seconds()
                if remaining <= 0:
                    break

        except KeyboardInterrupt:
            logger.info("Orchestrator: Interrupted by user")
        except Exception as e:
            logger.error(f"Orchestrator error: {e}")
            raise
        finally:
            self.disconnect_all()
            self.print_summary()

    def print_summary(self):
        """Print execution summary"""
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        avg_throughput = self.total_published / elapsed if elapsed > 0 else 0

        logger.info("")
        logger.info("=== Execution Summary ===")
        logger.info(f"Total Published: {self.total_published} messages")
        logger.info(f"Duration: {elapsed:.1f} seconds")
        logger.info(f"Average Throughput: {avg_throughput:.1f} msg/sec")
        logger.info("=== Orchestrator Complete ===")

    def stop(self):
        """Stop the orchestrator gracefully"""
        self.running = False


def main():
    """Main entry point"""
    orchestrator = GeneratorOrchestrator()
    
    try:
        # Run for 30 minutes (or set via GENERATION_CONFIG)
        orchestrator.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
