import { useState } from "react";
import { Navigate, useNavigate } from "react-router-dom";
import { useAuth } from "@contexts/authProvider.jsx";
import faviconLogo from "@assets/favicon.ico";
import axios from "axios";
import { TextInput, Button } from "flowbite-react";

const Login = () => {
  const { token, setToken } = useAuth();
  const navigate = useNavigate();

  const [email_id, setEmailId] = useState();
  const [password, setPassword] = useState();
  const [invalid, showInvalid] = useState(false);

  // Check if the user is already authenticated
  if (token) return <Navigate to="/dashboard" />;

  const handleLogin = async (event) => {
    event.preventDefault();

    try {
      const response = await axios({
        method: "POST",
        url: "/user/login",
        baseURL: `${env.API_ADDRESS}/`,
        data: { email_id: email_id, password: password },
        params: {},
        headers: { authorization: token },
        transformResponse: (response) => {
          return JSON.parse(response);
        },
      });

      setToken(response.headers["authorization"]);
      showInvalid(false);
      navigate("/dashboard", { replace: true });
    } catch (error) {
      // console.log("ERROR", error.status);
      setToken();
      showInvalid(true);
    }
  };

  return (
    <div className="h-screen flex items-center justify-center dark:bg-gray-700">
      <form
        className="p-16 border-spacing-28 border-8 border-black dark:border-gray-100 bg-zinc-200 dark:bg-gray-400"
        onSubmit={handleLogin}
      >
        <img
          src={faviconLogo}
          className="h-20 mx-auto mb-2 mt-6"
          alt="Artham"
        />
        <p className="mb-6 text-center text-4xl font-extrabold dark:text-gray-800">
          Artham
        </p>
        <TextInput
          className="mb-4 w-64 shadow-lg"
          id="email1"
          type="email"
          placeholder="Email ID"
          required
          onChange={(e) => setEmailId(e.target.value)}
        />
        <TextInput
          className="mb-4 w-64 shadow-lg"
          id="password1"
          type="password"
          placeholder="Password"
          required
          onChange={(e) => setPassword(e.target.value)}
        />
        <Button className="mx-auto shadow-lg" type="submit">
          LOG IN
        </Button>
        {invalid ? (
          <p className="h-4 mt-2 text-center text-red-600">
            Invalid Email ID or Password
          </p>
        ) : (
          <p className="h-4 mt-2"></p>
        )}
      </form>
    </div>
  );
};

export default Login;
