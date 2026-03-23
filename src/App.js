import React, { useEffect, useState } from "react";

const API_BASE = "https://ngx-backend.onrender.com";

function App() {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("123456");
  const [result, setResult] = useState("");
  const [loading, setLoading] = useState(false);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [data, setData] = useState(null);

  useEffect(() => {
    const token = localStorage.getItem("access_token");
    if (token) {
      setIsLoggedIn(true);

      fetch(`${API_BASE}/`)
        .then((res) => res.json())
        .then((d) => setData(d))
        .catch(() => setData({ error: "Failed to load" }));
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
        },
        body: JSON.stringify({ username, password }),
      });

      const data = await response.json();

      if (response.ok && data.access_token) {
        localStorage.setItem("access_token", data.access_token);
        setIsLoggedIn(true);
        setResult("Login successful");

        // fetch data after login
        fetch(`${API_BASE}/api/health`)
          .then((res) => res.json())
          .then((d) => setData(d));
      } else {
        setResult("Login failed");
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
  };

  if (isLoggedIn) {
    return (
      <div style={{ padding: "40px" }}>
        <h1>Dashboard</h1>
        <p>Welcome, {username}</p>

        <button onClick={handleLogout}>Logout</button>

        <h3>Backend Data:</h3>

        {data ? (
          <pre>{JSON.stringify(data, null, 2)}</pre>
        ) : (
          <p>Loading...</p>
        )}
      </div>
    );
  }

  return (
    <div style={{ padding: "40px" }}>
      <h1>Login</h1>

      <input
        value={username}
        onChange={(e) => setUsername(e.target.value)}
      />
      <br />
      <br />
      <input
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
      />
      <br />
      <br />

      <button onClick={handleLogin}>
        {loading ? "Loading..." : "Login"}
      </button>

      <p>{result}</p>
    </div>
  );
}

export default App;
