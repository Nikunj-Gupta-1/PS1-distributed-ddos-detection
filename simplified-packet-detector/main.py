# file: main.py
import click
import yaml
import os
import logging
from src.utils.logger import setup_logging
from src.capture.packet_capture import DistributedPacketSniffer


@click.command()
@click.option(
    "--iface",
    default="auto",
    help="Interface name (e.g. eth0) or 'auto' for first non-loopback",
)
@click.option("--topic", default="raw_packets", help="Kafka topic name")
@click.option(
    "--config",
    default="config/kafka_config.yaml",
    help="Path to kafka_config.yaml",
)
@click.option(
    "--kafka-servers",
    default=None,
    help="Kafka bootstrap servers (comma-separated). Overrides config file.",
)
@click.option(
    "--device-id",
    default=None,
    help="Unique device identifier. Auto-generated if not provided.",
)
def run(iface, topic, config, kafka_servers, device_id):
    """
    Start distributed packet capture ➜ send raw packets to Kafka.
    Designed for deployment on multiple devices across a network.
    """
    setup_logging("config/logging_config.yaml")
    logger = logging.getLogger(__name__)

    # Determine Kafka servers
    if kafka_servers:
        servers = kafka_servers.split(',')
        logger.info(f"Using Kafka servers from command line: {servers}")
    else:
        # Load from config file
        try:
            with open(config, "r") as fp:
                k_conf = yaml.safe_load(fp)["kafka"]
            servers = k_conf["bootstrap_servers"]
            logger.info(f"Using Kafka servers from config: {servers}")
        except Exception as e:
            # Fallback to environment variable or localhost
            servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')
            logger.warning(f"Config file not found, using fallback: {servers}")

    # Override topic from environment if set
    topic = os.getenv("KAFKA_TOPIC", topic)
    
    logger.info(f"Starting distributed packet capture:")
    logger.info(f"  Interface: {iface}")
    logger.info(f"  Kafka servers: {servers}")
    logger.info(f"  Kafka topic: {topic}")
    logger.info(f"  Device ID: {device_id or 'auto-generated'}")

    # Create and start distributed sniffer
    sniffer = DistributedPacketSniffer(
        interface=iface if iface != "auto" else None,
        kafka_servers=servers,
        kafka_topic=topic,
        device_id=device_id
    )
    
    try:
        sniffer.start_sniffing()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Print final statistics
        stats = sniffer.get_stats()
        logger.info("Final statistics:")
        logger.info(f"  Device: {stats['device_id']}")
        logger.info(f"  Packets captured: {stats['capture_stats']['packets_captured']}")
        logger.info(f"  Packets sent: {stats['capture_stats']['packets_sent']}")
        logger.info(f"  Capture rate: {stats['capture_rate']:.1f} pps")
        logger.info(f"  Send rate: {stats['send_rate']:.1f} pps")
        logger.info(f"  Kafka errors: {stats['kafka_stats']['errors']}")


if __name__ == "__main__":
    run()

