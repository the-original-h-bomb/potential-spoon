CREATE OR REPLACE FUNCTION "DUMMY_FUNCTION"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS '
    return "This is a dummy function for testing.";
  ';