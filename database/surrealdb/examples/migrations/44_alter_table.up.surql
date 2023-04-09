-- Make sure that email addresses in the user table are always unique
DEFINE INDEX userEmailIndex ON TABLE user COLUMNS email UNIQUE;
