# packr

This driver enables you to read migrations from [packr v2](https://github.com/gobuffalo/packr/tree/master/v2).
Unfortunately there is currently no possibility to address packr boxes via an URL, so the `Driver.Open` function
will always return an error. Instead please use `WithInstance` to instantiate an Instance of this driver.