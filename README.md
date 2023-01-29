# Restaurant booking system API

- [Restaurant booking system API](#restaurant-booking-system-api)
  - [Requirements](#requirements)
  - [Setup](#setup)
  - [Usage](#usage)
  - [Task: Create endpoints to cover the following scenarios](#task-create-endpoints-to-cover-the-following-scenarios)
    - [Support bookings](#support-bookings)
    - [Restaurant Management](#restaurant-management)
    - [Support Waiters](#support-waiters)
    - [Support Kitchen](#support-kitchen)

## Requirements

- Pandas
- python-dateutil
- Python>=3.7 or newer

## Setup

```bash
conda create -n myenv --file ENV.yml
conda activate myenv
```

## Usage

```bash
# To show examples of all the scenarios
python dev.py --log-lvl info 
```

```bash
# For continuous usage
python prod.py --log-lvl info 
```

## Task: Create endpoints to cover the following scenarios

- [x] Python 3.7 or newer
- [ ] Use a web framework such as FastAPI
- [x] Use a dabatase for storage

### Support bookings

- [x] Book tables
- [x] Changing bookings
- [x] Cancel bookings
- [x] Get all bookings
- [x] See available tables

### Restaurant Management

- [x] create the menu with pricing (eg: pizza £10)

### Support Waiters

- [x] Provide menu
- [x] Take orders from a table (don't allow duplicate orders for the same table)

### Support Kitchen

- [x] See the existing orders (by time, first in first out)
- [x] Update status of orders (prepping, cooking, ready)
