# Blockchain Event Scanner and Transaction Extractor


A robust system to scan and extract transaction data for a specific contract address on the Ethereum blockchain. This project involved creating an efficient blockchain event scanner to capture and process transaction logs and store the data in a database for further analysis.

## Key features
- State Management: Implemented a state management system, the state manager tracks and stores the last state of scanned blocks.
- Dynamic chunk size: The data processing pipeline dynamically adapts the scan chunk size based on retry logic and processing times, ensuring efficient and reliable data extraction.
