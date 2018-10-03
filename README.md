# Purpose

This application will query the `sierra-view.hold` table, and store the items on the shelf that are ready for pickup. It will also record the changes to the row, including deletion time.

## Overview

        This script is designed to be run at regular intervals (perhaps every 5 minutes, or less?).

        It will store data in a local db (sqlite) so that it may be queried at a later date to determine previous item locations (for use in computing item delievery paths).