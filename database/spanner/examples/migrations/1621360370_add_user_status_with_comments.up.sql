-- Migration: Add status column to Users table
-- Author: migrate
-- Description: This migration adds a status column to track user account state

/*
 * The Status column will store the current state of the user account.
 * Valid values: 'active', 'inactive', 'suspended'
 */
ALTER TABLE Users ADD COLUMN Status STRING(20); -- default will be NULL
