import axios from "axios";
import { useAuth } from "@contexts/authProvider.jsx";
import { apiAddress } from "@/config/runtime.js";

export function requestApi() {
  const { token } = useAuth(); // token directly from your context

  // Create axios instance WITH baseURL
  const instance = axios.create({
    baseURL: `${apiAddress}/`,
    timeout: 60000,
    responseType: "json",
  });

  // Attach token to all requests
  instance.interceptors.request.use((config) => {
    if (token) {
      config.headers.authorization = token;
    }
    return config;
  });

  // Normalize all errors to one shape
  instance.interceptors.response.use(
    (res) => res,
    (err) => {
      return Promise.reject({
        status: err?.response?.status,
        data: err?.response?.data,
        message: err?.message,
      });
    }
  );

  // Main request function you will use
  const request = async (endpoint, method = "GET", params = {}, data = {}, config = {}) => {
    const response = await instance({
      url: endpoint,
      method,
      params,
      data,
      ...config,
    });

    return response.data;
  };

  return { request };
}
