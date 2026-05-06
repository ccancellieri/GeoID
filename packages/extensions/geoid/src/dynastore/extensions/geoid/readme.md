# GeoID - Global Federated Places Service

GeoID is an open-source, OGC-compliant infrastructure designed to manage, store, and share billions of geospatial locations (places) efficiently. It serves as a Digital Public Good, promoting federated collaboration and high-performance access to geospatial intelligence.

## Features

- **OGC API Features Support**: Conformant with modern Open Geospatial Consortium standards for vector data.
- **Decentralized Identifiers (DIDs)**: Each place is issued a unique, secure, and immutable DID for persistent tracking.
- **Data Provenance**: Comprehensive tracking of data origin, processing steps, and authority management.
- **High-Performance Querying**: Optimized for planetary-scale discovery using STAC and advanced spatial indexing (S2, H3).
- **Federated Architecture**: Built for replication and consistency across distributed nodes.
- **Geoid Height Utility**: Integrated tool for precision vertical positioning correction (EGM2008).

## Usage

1. **Activation**: Enable the GeoID extension in your service configuration.
2. **Access**: Navigate to the `/web/geoid` endpoint to access the brand page and calculator.
3. **API**: Use the OGC-compliant endpoints for bulk ingestion and high-frequency querying.
