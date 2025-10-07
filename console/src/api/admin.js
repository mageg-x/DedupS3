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
  get:
    (url, defaultMsg) =>
    async (params, config = {}) => {
      let finalUrl = url;

      // 只有当 params 存在且有属性时才拼接 query
      if (params && Object.keys(params).length > 0) {
        const search = new URLSearchParams();
        for (const [key, value] of Object.entries(params)) {
          if (value !== null && value !== undefined) {
            search.append(key, value);
          }
        }
        const queryString = search.toString();
        if (queryString) {
          finalUrl = `${url}?${queryString}`;
        }
      }

      try {
        const res = await api.get(finalUrl, config);
        return res.data;
      } catch (error) {
        const response = error.response;
        if (response) {
          const msg =
            response.data?.msg || response.data?.message || defaultMsg;
          return { success: false, message: msg };
        }
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

  delete:
    (url, defaultMsg) =>
    async (params, config = {}) => {
      let finalUrl = url;

      if (params && Object.keys(params).length > 0) {
        const search = new URLSearchParams();
        for (const [key, value] of Object.entries(params)) {
          if (value !== null && value !== undefined) {
            search.append(key, value);
          }
        }
        const queryString = search.toString();
        if (queryString) {
          finalUrl = `${url}?${queryString}`;
        }
      }

      try {
        const res = await api.delete(finalUrl, config);
        return res.data;
      } catch (error) {
        const response = error.response;
        if (response) {
          const msg =
            response.data?.msg || response.data?.message || defaultMsg;
          return { success: false, message: msg };
        }
        return { success: false, message: defaultMsg };
      }
    },
  upload:
    (url, defaultMsg) =>
    async (formData, config = {}) => {
      try {
        // 确保 headers 正确设置为 multipart
        const finalConfig = {
          headers: {
            "Content-Type": "multipart/form-data",
            ...config.headers,
          },
          ...config,
        };

        const res = await api.post(url, formData, finalConfig);
        return res.data;
      } catch (error) {
        const response = error.response;
        const msg =
          response?.data?.msg ||
          response?.data?.message ||
          defaultMsg ||
          "File upload failed";
        return { success: false, message: msg };
      }
    },
download:
  (url, defaultMsg) =>
  async (params = {}, config = {}) => {
    try {
      const finalConfig = {
        ...config,
        responseType: "blob",
      };

      const res = await api.post(url, params, finalConfig);
      const contentType = res.headers["content-type"];

      // 处理后端返回的错误（JSON 或文本）
      if (
        contentType &&
        (contentType.includes("application/json") ||
          contentType.includes("text/plain"))
      ) {
        let errorMsg = defaultMsg || "Download failed";
        try {
          const text = await res.data.text();
          try {
            const errorObj = JSON.parse(text);
            errorMsg = errorObj.msg || errorObj.message || errorMsg;
          } catch {
            // 非 JSON，直接使用文本（可选）
            errorMsg = errorMsg; // 保持默认
          }
        } catch (e) {
          // 读取失败
        }
        return { success: false, message: errorMsg };
      }

      // 提取文件名：优先从 header，其次 params.filename，最后默认
      let filename = "download.zip";
      const disposition = res.headers["content-disposition"];
      if (disposition) {
        const match = disposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
        if (match != null && match[1]) {
          filename = match[1].replace(/['"]/g, "");
        }
      } else if (params.filename) {
        // 清理文件名：移除不安全字符
        filename = params.filename.replace(/[<>:"/\\|?*\x00-\x1F]/g, "_");
      }

      // 创建 Blob 并触发下载
      const blob = new Blob([res.data], { type: contentType });
      const blobUrl = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = blobUrl;
      a.download = filename;
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(blobUrl); // 立即释放

      return { success: true, message: "Download started" };
    } catch (error) {
      return { success: false, message: defaultMsg || "Download failed" };
    }
  },
};

export const getstats = apicall.get("/stats", "Error fetching stats");
export const listbuckets = apicall.get("/bucket/list", "Error listing buckets");
export const createbucket = apicall.put("/bucket/create","Error creating bucket");
export const deletebucket = apicall.delete("/bucket/delete", "Error deleting bucket");
export const listobjects = apicall.get("/bucket/objects", "Error listing objects");
export const createfolder = apicall.put("/bucket/folder", "Error creating folder");
export const putobject = apicall.upload("/bucket/putobject", "Failed to put object");
export const delobject = apicall.post("/bucket/deleteobject", "Failed to delete object");
export const getobject = apicall.download("/bucket/getobject", "Failed to download file");