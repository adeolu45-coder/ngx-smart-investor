import React, { useEffect, useState } from "react";

const API_BASE = "https://ngx-backend.onrender.com";

function App() {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("123456");
  const [result, setResult] = useState("");
  const [loading, setLoading] = useState(false);
  const [isLoggedIn, setIsLoggedIn] = useState(false);

  useEffect(() => {
    const token = localStorage.getItem("access_token");
    if (token) {
      setIsLoggedIn(true);
    }
  }, []);

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    setResult("");

    try {
      const response = await fetch(`${API_BASE}/api/auth/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ username, password }),
      });

      const data = await response.json();

      if (response.ok && data.access_token) {
        localStorage.setItem("access_token", data.access_token);
        setIsLoggedIn(true);
        setResult("Login successful.");
      } else {
        setResult(data.detail || "Login failed");
      }
    } catch (error) {
      setResult("Network error");
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem("access_token");
    setIsLoggedIn(false);
    setResult("Logged out.");
    setPassword("");
  };

  if (isLoggedIn) {
    return (
      <div style={{ padding: "40px", fontFamily: "Arial, sans-serif" }}>
        <h1>NGX Smart Investor Dashboard</h1>
        <p>Welcome, {username}.</p>

        <div style={{ marginTop: "20px" }}>
          <button onClick={handleLogout} style={{ padding: "10px 16px" }}>
            Logout
          </button>
        </div>

        <div style={{ marginTop: "30px" }}>
          <h2>Next features coming</h2>
          <p>• Market data</p>
          <p>• Stock prices</p>
          <p>• Signals</p>
          <p>• User management</p>
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: "40px", fontFamily: "Arial, sans-serif" }}>
      <h1>NGX Smart Investor</h1>

      <form onSubmit={handleLogin} style={{ maxWidth: "400px" }}>
        <div style={{ marginBottom: "12px" }}>
          <label>Username</label>
          <br />
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            style={{ width: "100%", padding: "8px" }}
          />
        </div>

        <div style={{ marginBottom: "12px" }}>
          <label>Password</label>
          <br />
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            style={{ width: "100%", padding: "8px" }}
          />
        </div>

        <button type="submit" disabled={loading} style={{ padding: "10px 16px" }}>
          {loading ? "Logging in..." : "Login"}
        </button>
      </form>

      {result && <p style={{ marginTop: "20px" }}>{result}</p>}
    </div>
  );
}

export default App;
