import React, { useEffect, useState } from "react";

const API_BASE = "https://ngx-backend.onrender.com";

function App() {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("123456");
  const [result, setResult] = useState("");
  const [loading, setLoading] = useState(false);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [data, setData] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastChecked, setLastChecked] = useState("");

  useEffect(() => {
    const token = localStorage.getItem("access_token");
    if (token) {
      setIsLoggedIn(true);
      fetchStatus(token);
    }
  }, []);

  const fetchStatus = async (tokenFromArg) => {
    const token = tokenFromArg || localStorage.getItem("access_token");
    if (!token) return;

    setRefreshing(true);
    try {
      const res = await fetch(`${API_BASE}/api/status`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const result = await res.json();
      setData(result);
      setLastChecked(new Date().toLocaleString());
    } catch (err) {
      setData({ error: "Failed to load" });
      setLastChecked(new Date().toLocaleString());
    } finally {
      setRefreshing(false);
    }
  };

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

      const loginData = await response.json();

      if (response.ok && loginData.access_token) {
        localStorage.setItem("access_token", loginData.access_token);
        setIsLoggedIn(true);
        setResult("Login successful.");
        fetchStatus(loginData.access_token);
      } else {
        setResult(loginData.detail || "Login failed");
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
    setData(null);
    setLastChecked("");
    setResult("Logged out.");
    setPassword("");
  };

  if (isLoggedIn) {
    return (
      <div style={{ padding: "40px", fontFamily: "Arial, sans-serif" }}>
        <h1>NGX Smart Investor Dashboard</h1>
        <p>Welcome, {username}.</p>

        <div style={{ marginTop: "20px", marginBottom: "20px" }}>
          <button onClick={handleLogout} style={{ padding: "10px 16px", marginRight: "10px" }}>
            Logout
          </button>

          <button onClick={() => fetchStatus()} style={{ padding: "10px 16px" }}>
            {refreshing ? "Refreshing..." : "Refresh Data"}
          </button>
        </div>

        <div style={{ marginTop: "20px" }}>
          <p><strong>Market data type:</strong> End-of-day (not live)</p>
          <p><strong>Best update time:</strong> After 6:30 PM WAT</p>
          {lastChecked && <p><strong>Last checked:</strong> {lastChecked}</p>}
        </div>

        <div style={{ marginTop: "30px" }}>
          <h2>Backend Status</h2>

          {data ? (
            <pre style={{ background: "#f4f4f4", padding: "16px", borderRadius: "8px" }}>
              {JSON.stringify(data, null, 2)}
            </pre>
          ) : (
            <p>Loading data...</p>
          )}
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
