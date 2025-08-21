const express = require("express");
const app = express();

// Cloud Run sets the port in process.env.PORT
const PORT = process.env.PORT || 8080;

app.get("/", (req, res) => {
  res.send("Hello from Cloud Run + Node.js!");
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
