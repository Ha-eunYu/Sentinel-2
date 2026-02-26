![Python](https://img.shields.io/badge/Python-3.10-blue)  
![License: MIT](https://img.shields.io/badge/License-MIT-green)

# Sentinel-2 Optical Reference Pipeline

A reproducible pipeline to generate RGB composites and spectral indices (NDVI, NDWI, MNDWI) from Sentinel-2 Level-2A imagery using Copernicus Data Space STAC search and S3 access.

⚠️ **This repository does NOT contain Sentinel-2 imagery.**
All data must be downloaded directly from the Copernicus Data Space Ecosystem.



## Overview

The pipeline performs:

1. STAC search with cloud filtering and a date window
2. Download of required bands via S3
3. RGB image generation (TCI and/or B02/B03/B04 composite)
4. Index computation:
   - NDVI
   - NDWI (McFeeters)
   - MNDWI
5. SCL-based masking (cloud/shadow/snow removal)
6. Export to GeoTIFF and PNG quicklooks



## Data source

- **Sentinel-2 Level-2A (Surface Reflectance)**
- Accessed via the **Copernicus Data Space Ecosystem**

This repository does not redistribute Sentinel-2 data.
When publishing derived products, follow your journal/institution guidelines for Copernicus/Sentinel attribution.



## Repository structure

.
├── s2_stac_pick_cloudfree.py
├── s2_s3_download_rgb.py
├── s2_make_rgb.py
├── s2_make_indices.py
├── environment.yml
└── downloads/  # (ignored) local data and outputs



## Required bands

| Product | Bands Used |
|--------|------------|
| RGB | B02, B03, B04 (10 m) |
| NDVI | B08 (10 m), B04 (10 m) |
| NDWI | B03 (10 m), B08 (10 m) |
| MNDWI | B03 (10 m), B11 (20 m → resampled) |
| Mask | SCL (20 m → nearest resampled) |

All outputs are standardized to **10 m** resolution.



## Notes

- 20 m layers (B11, SCL) are read on the 10 m reference grid (B03_10m) using on-the-fly resampling.
- Bilinear resampling is used for continuous reflectance bands (e.g., B11).
- Nearest-neighbor resampling is used for categorical layers (SCL).
- Pixels flagged as cloud/shadow/snow/invalid in SCL are masked (set to NaN).
- Indices are exported as float32 GeoTIFF (GTiff) and PNG quicklooks.



## Requirements  
  
- Python: 3.10+  
- Conda: Miniconda/Anaconda recommended (use conda-forge)  
- Environments:  
- `s2api`: STAC search + S3 download  
- `s2viz`: processing + visualization (GDAL/Rasterio stack)  
  
All dependencies are pinned in `environment_*.yml` (single source of truth).



## Setup

Create environment:

conda env create -f environment.yml
conda activate s2viz

Create a `.env` file:

CDSE_S3_ACCESS_KEY=your_key
CDSE_S3_SECRET_KEY=your_secret



## Workflow

### 1) Scene selection

python s2_stac_pick_cloudfree.py

### 2) Download required bands

python s2_s3_download_rgb.py

### 3) Generate RGB composite

python s2_make_rgb.py

### 4) Compute spectral indices

python s2_make_indices.py

Outputs are written to:

downloads/S2_INDICES/



## Intended use: SAR validation (optical reference)

These Sentinel-2 products are intended as *optical reference layers* to support SAR-based analysis (e.g., MicroSAR / small-SAR satellite studies), for example:

- Qualitative comparison of SAR-derived water masks vs. optical water proxies (NDWI/MNDWI)
- Seasonal context layers for SAR acquisitions (vegetation state via NDVI)
- Cloud-contamination screening using SCL (to avoid misinterpreting optical indices)

**Important:** spectral indices are not absolute ground truth for water.
They can be biased by turbidity, algae blooms, sunglint, terrain shadows, mixed pixels, and adjacency effects.



## Citation

If you use this repository in academic work, please cite the software and cite the Sentinel-2 data source separately.

### Data citation (guidance)
Sentinel-2 data are provided by the European Union’s Copernicus Programme via the Copernicus Data Space Ecosystem.
Follow your journal/institution guidelines for Copernicus/Sentinel attribution.

### Software citation (recommended metadata)
- Author: Ha-eun Yu
- Title: Sentinel-2 Optical Reference Pipeline
- Year: 2026
- Version: v0.1.0
- URL: https://github.com/Ha-eunYu/s2-optical-reference-pipeline



## License

This project is licensed under the MIT License.
See the LICENSE file for details.
