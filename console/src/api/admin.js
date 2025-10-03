import axios from 'axios';
import md5 from 'blueimp-md5';

const api = axios.create({
  baseURL: 'http://172.17.179.50:3002/api', // 自动相对于当前域名
  headers: {
    'Content-Type': 'application/json',
  },
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
    const LOGIN_PAGES = ['/login', '/auth', '/register'];

    // 3. 如果是 401 且当前不在登录页，则跳转
    if (error.response?.status === 401) {
      if (!LOGIN_PAGES.includes(currentPath)) {
        console.warn('Token expired or invalid. Redirecting to login...');
        logout(); // 清理状态
        window.location.href = '/login';
      } else {
        // 在登录页发生的 401 → 不跳转，返回错误给 UI 显示
        console.warn('Login failed:', error.response?.data?.message);
      }
    }

    return Promise.reject(error);
  }
);

export async function login(username, password) {
  try {
    // 对password进行md5处理
    password = md5(password + ":" + username);
    const res = await api.post('/login', { username, password });
    console.log(res.data);
    if (res.data.code == 0) {
      return { success: true, message: res.data.msg };
    } else {
      return { success: true, message: res.data.msg };
    }
  } catch (error) {
    const msg = error.response?.data?.message || '登录失败...';
    return { success: false, message: msg };
  }
}

export async function logout() {
  // 1. 尝试调用登出接口（清除 HttpOnly Cookie）
  try {
    await api.post('/logout', {}, {
      timeout: 2000 // 设置短超时，避免卡住
    });
  } catch (error) {
    // 忽略错误：网络失败或服务不可用
    console.warn('Logout API failed, but proceeding...');
  } finally {
    // 2. 无论后端是否成功，都跳转
    window.location.href = '/login';
  }
}