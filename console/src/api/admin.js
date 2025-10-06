import axios from "axios";
import md5 from "blueimp-md5";

axios.defaults.withCredentials = true;
const api = axios.create({
  baseURL: "/api", // 自动相对于当前域名
  headers: {
    "Content-Type": "application/json",
  },
  withCredentials: true,
});

api.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    const config = error.config;

    // 1. 获取当前页面路径（不是请求的 API 路径）
    const currentPath = window.location.pathname;

    // 2. 定义登录相关页面（白名单：这些页面即使 401 也不跳转）
    const LOGIN_PAGES = ["/login", "/auth", "/register"];

    // 3. 如果是 401 且当前不在登录页，则跳转
    if (error.response?.status === 401) {
      if (!LOGIN_PAGES.includes(currentPath)) {
        console.warn("Token expired or invalid. Redirecting to login...");
        logout(); // 清理状态
        window.location.href = "/login";
      } else {
        // 在登录页发生的 401 → 不跳转，返回错误给 UI 显示
        console.warn("Login failed:", error.response?.data?.message);
      }
    }

    return Promise.reject(error);
  }
);

export async function login(username, password) {
  try {
    // 对password进行md5处理
    password = md5(password + ":" + username);
    const res = await api.post("/login", { username, password });
    console.log(res.data);
    if (res.data.code == 0) {
      return { success: true, message: res.data.msg };
    } else {
      return { success: true, message: res.data.msg };
    }
  } catch (error) {
    const msg = error.response?.data?.message || "登录失败...";
    return { success: false, message: msg };
  }
}

export async function logout() {
  // 1. 尝试调用登出接口（清除 HttpOnly Cookie）
  try {
    await api.post(
      "/logout",
      {},
      {
        timeout: 2000, // 设置短超时，避免卡住
      }
    );
  } catch (error) {
    // 忽略错误：网络失败或服务不可用
    console.warn("Logout API failed, but proceeding...");
  } finally {
    // 2. 无论后端是否成功，都跳转
    window.location.href = "/login";
  }
}

// 包装代码（可多行，只写一次）
const apicall = {
  get: (url, defaultMsg) => async (config) => {
    try {
      const res = await api.get(url, config);
      return res.data; // 假设 2xx 时 data 是 { success: true, ... }
    } catch (error) {
      // 网络错误 或 非 2xx 响应（axios 会 reject）
      const response = error.response;
      if (response && (response.status < 200 || response.status >= 300)) {
        // 优先使用后端返回的 msg
        const msg = response.data?.msg || response.data?.message || defaultMsg;
        return { success: false, message: msg };
      }
      // 网络错误、超时等
      return { success: false, message: defaultMsg };
    }
  },

  post: (url, defaultMsg) => async (data, config) => {
    try {
      const res = await api.post(url, data, config);
      return res.data;
    } catch (error) {
      const response = error.response;
      if (response) {
        const msg = response.data?.msg || response.data?.message || defaultMsg;
        return { success: false, message: msg };
      }
      return { success: false, message: defaultMsg };
    }
  },

  put: (url, defaultMsg) => async (data, config) => {
    try {
      const res = await api.put(url, data, config);
      return res.data;
    } catch (error) {
      const response = error.response;
      if (response) {
        const msg = response.data?.msg || response.data?.message || defaultMsg;
        return { success: false, message: msg };
      }
      return { success: false, message: defaultMsg };
    }
  },

 delete: (url, defaultMsg) => async (data, config) => {
    try {
      // 将 data 放在请求体中
      const res = await api.delete(url, {
        ...config,
        data,
      });
      return res.data;
    } catch (error) {
      const response = error.response;
      if (response) {
        const msg = response.data?.msg || response.data?.message || defaultMsg;
        return { success: false, message: msg };
      }
      return { success: false, message: defaultMsg };
    }
  },
};

export const getstats = apicall.get("/stats", "Error fetching stats");
export const listbuckets = apicall.get("/listbuckets", "Error listing buckets");
export const createbucket = apicall.put("/createbucket","Error creating bucket");
export const deletebucket = apicall.delete( "/deletebucket", "Error deleting bucket");
