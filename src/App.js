import React, { useEffect, useState } from "react";

const API_BASE = "https://ngx-backend.onrender.com";

export default function App() {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("123456");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState("");
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [statusData, setStatusData] = useState(null);

  const fetchStatus = async (tokenArg) => {
  const token = tokenArg || localStorage.getItem("access_token");

  try {
    const res = await fetch(`${API_BASE}/api/status`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    const data = await res.json();
    setStatusData({
      httpStatus: res.status,
      ok: res.ok,
      data: data,
    });
  } catch (error) {
    setStatusData({
      error: "Failed to load",
      details: String(error),
    });
  }
};

  useEffect(() => {
    const token = localStorage.getItem("access_token");
    if (token) {
      setIsLoggedIn(true);
      fetchStatus(token);
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
        alert(data.access_token);
        setIsLoggedIn(true);
        setResult("Login successful");
        fetchStatus(data.access_token);
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
    setStatusData(null);
    setResult("");
  };

  const pageStyle = {
    minHeight: "100vh",
    background: "linear-gradient(180deg, #081a3a 0%, #102a52 100%)",
    color: "#ffffff",
    fontFamily: "Arial, sans-serif",
    padding: "20px",
  };

  const cardStyle = {
    background: "rgba(255,255,255,0.06)",
    border: "1px solid rgba(255,255,255,0.12)",
    borderRadius: "18px",
    padding: "20px",
    marginBottom: "18px",
  };

  const buttonStyle = {
    padding: "12px 18px",
    borderRadius: "12px",
    border: "none",
    cursor: "pointer",
    fontWeight: "bold",
  };

  if (!isLoggedIn) {
    return (
      <div style={pageStyle}>
        <div style={{ maxWidth: "420px", margin: "60px auto" }}>
          <h1 style={{ fontSize: "42px", marginBottom: "10px" }}>NGX Smart Investor</h1>
          <p style={{ color: "#c7d2e3", marginBottom: "24px" }}>
            Login to access your dashboard
          </p>

          <div style={cardStyle}>
            <form onSubmit={handleLogin}>
              <div style={{ marginBottom: "14px" }}>
                <label>Username</label>
                <input
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "12px",
                    marginTop: "8px",
                    borderRadius: "10px",
                    border: "1px solid #415a77",
                    background: "#0f223f",
                    color: "#fff",
                  }}
                />
              </div>

              <div style={{ marginBottom: "18px" }}>
                <label>Password</label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "12px",
                    marginTop: "8px",
                    borderRadius: "10px",
                    border: "1px solid #415a77",
                    background: "#0f223f",
                    color: "#fff",
                  }}
                />
              </div>

              <button
                type="submit"
                disabled={loading}
                style={{
                  ...buttonStyle,
                  width: "100%",
                  background: "#1ec971",
                  color: "#fff",
                }}
              >
                {loading ? "Logging in..." : "Login"}
              </button>
            </form>

            {result && <p style={{ marginTop: "16px" }}>{result}</p>}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={pageStyle}>
      <div style={{ maxWidth: "1100px", margin: "0 auto" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: "20px",
            flexWrap: "wrap",
            gap: "12px",
          }}
        >
          <div>
            <h1 style={{ margin: 0, fontSize: "42px" }}>NGX Smart Investor</h1>
            <p style={{ margin: "8px 0 0", color: "#c7d2e3" }}>Welcome, {username}</p>
          </div>

          <div style={{ display: "flex", gap: "10px" }}>
            <button
              onClick={() => fetchStatus()}
              style={{ ...buttonStyle, background: "#1ec971", color: "#fff" }}
            >
              {refreshing ? "Refreshing..." : "Refresh"}
            </button>
            <button
              onClick={handleLogout}
              style={{ ...buttonStyle, background: "#31445f", color: "#fff" }}
            >
              Logout
            </button>
          </div>
        </div>

        <div style={cardStyle}>
          <h2 style={{ marginTop: 0 }}>Best Trade of the Day</h2>
          <h1 style={{ fontSize: "54px", margin: "0 0 8px" }}>SEPLAT</h1>
          <p style={{ color: "#c7d2e3", marginTop: 0 }}>SEPLAT ENERGY PLC</p>

          <div style={{ display: "flex", flexWrap: "wrap", gap: "16px", marginTop: "20px" }}>
            <div style={{ ...cardStyle, marginBottom: 0, minWidth: "220px", flex: 1 }}>
              <div style={{ color: "#9fb3c8" }}>Current Price</div>
              <div style={{ fontSize: "42px", fontWeight: "bold" }}>₦9,099.90</div>
            </div>

            <div style={{ ...cardStyle, marginBottom: 0, minWidth: "220px", flex: 1 }}>
              <div style={{ color: "#9fb3c8" }}>Opportunity Score</div>
              <div style={{ fontSize: "42px", fontWeight: "bold", color: "#29d98a" }}>8.12/10</div>
            </div>
          </div>

          <div style={{ marginTop: "16px" }}>
            <span
              style={{
                padding: "10px 14px",
                borderRadius: "999px",
                background: "rgba(30,201,113,0.18)",
                border: "1px solid #1ec971",
                color: "#67f0ab",
                fontWeight: "bold",
              }}
            >
              Confidence: Very High
            </span>
          </div>
        </div>

        <div style={cardStyle}>
          <h2 style={{ marginTop: 0 }}>Backend Status</h2>
          <pre
            style={{
              background: "rgba(255,255,255,0.06)",
              padding: "16px",
              borderRadius: "12px",
              overflowX: "auto",
              color: "#dbe8f5",
            }}
          >
            {JSON.stringify(statusData, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
}
