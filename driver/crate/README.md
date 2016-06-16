# Crate driver

This is a driver for the [Crate](https://crate.io) database. It is based on the Crate
sql driver by [herenow](https://github.com/herenow/go-crate).

This driver does not use transactions! This is not a limitation of the driver, but a 
limitation of Crate. So handle situations with failed migrations with care!

## Usage

```bash
migrate -url http://host:port -path ./db/migrations create add_field_to_table
migrate -url http://host:port -path ./db/migrations up
migrate help # for more info
```