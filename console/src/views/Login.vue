<template>
    <div class="login-page">
        <!-- 背景装饰元素 -->
        <div class="background-elements">
            <div class="grid-pattern"></div>
            <div class="radial-gradient"></div>
            <div class="polygon polygon-1"></div>
            <div class="polygon polygon-2"></div>
            <div class="polygon polygon-3"></div>
            <div class="polygon polygon-4"></div>
            <div class="tech-line line-1"></div>
            <div class="tech-line line-2"></div>
        </div>

        <div class="content-wrapper">
            <!-- 关于链接和GitHub链接 -->
            <div class="about-link-container mr-16 mt-8 z-10">
                <a href="#" @click.prevent="goToAbout" class="about-link mr-4">
                    <i class="fas fa-info-circle mr-1"></i>{{ t('login.about') }}
                </a>
            </div>
            <div class="github-link-container mr-16 mt-8 z-10">
                <a href="https://github.com/mageg-x/DedupS3" target="_blank" rel="noopener noreferrer"
                    class="about-link">
                    <i class="fab fa-github mr-1"></i>{{ t('login.github') }}
                </a>
            </div>
            <!-- 语言切换 -->
            <div class="language-switch-container mr-16 mt-8 z-10">
                <LanguageSwitch :sidebar-collapsed="false" />
            </div>

            <!-- 介绍区域 -->
            <div class="intro-section">
                <div class="logo">
                    <div class="logo-icon">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
                        </svg>
                    </div>
                    <div class="logo-text">{{ t('login.brandName') }}</div>
                </div>

                <h1 class="tagline">{{ t('login.tagline') }}</h1>
                <p class="subtitle">{{ t('login.subtitleDescription') }}</p>

                <!-- 特性展示 -->
                <div class="features">
                    <div v-for="(feature, index) in featuresData" :key="index"
                        class="feature flex flex-col items-center">
                        <div class="flex items-center w-full gap-x-4">
                            <div class="feature-icon">
                                <i :class="feature.icon"></i>
                            </div>
                            <div class="feature-header">
                                <div class="feature-title">{{ t(feature.titleKey) }}</div>
                            </div>
                        </div>
                        <div class="feature-content">
                            <p class="feature-description">{{ t(feature.descriptionKey) }}</p>
                        </div>
                    </div>
                </div>

                <!-- 技术图标 -->
                <div class="tech-logos">
                    <div class="tech-logo" v-for="(logo, index) in techLogos" :key="index">
                        <svg :width="logo.width" :height="logo.height" :viewBox="logo.viewBox" fill="none"
                            xmlns="http://www.w3.org/2000/svg">
                            <path v-for="(path, pathIndex) in logo.paths" :key="pathIndex" :d="path.d"
                                :fill="path.fill" />
                        </svg>
                    </div>
                </div>
            </div>

            <!-- 登录表单区域 -->
            <div class="login-section">
                <div class="login-card">
                    <!-- 添加隐藏的自动填充表单用于浏览器密码管理 -->
                    <form v-show="false" id="autofill-form">
                        <input type="text" name="username" v-model="loginForm.username" autocomplete="username">
                        <input type="password" name="password" v-model="loginForm.password" autocomplete="password">
                        <input type="password" name="secretKey" v-model="loginForm.secretKey" autocomplete="password">
                    </form>

                    <div class="login-header">
                        <div class="login-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"
                                stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                    d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z" />
                            </svg>
                        </div>
                        <h2 class="login-title">{{ t('login.title') }}</h2>
                        <p class="login-subtitle">{{ t('login.subtitle') }}</p>
                    </div>

                    <!-- 登录方式切换 -->
                    <div class="login-method-toggle">
                        <div :class="['method-btn', { active: loginMethod === 'iam' }]" @click="setLoginMethod('iam')">
                            {{ t('login.iamLogin') }}
                        </div>
                        <div :class="['method-btn', { active: loginMethod === 's3' }]" @click="setLoginMethod('s3')">
                            {{ t('login.s3Login') }}
                        </div>
                    </div>

                    <!-- 登录表单 -->
                    <form @submit.prevent="handleLogin" autocomplete="on" id="login-form">
                        <el-form ref="loginFormRef" :model="loginForm" :rules="rules" label-position="top">
                            <!-- S3凭证登录表单 -->
                            <div v-if="loginMethod === 's3'">
                                <el-form-item :label="t('login.accessKeyId')" prop="accessKeyId">
                                    <el-input v-model="loginForm.accessKeyId"
                                        :placeholder="t('login.accessKeyPlaceholder')" size="large"
                                        autocomplete="username" id="accessKeyId" name="accessKeyId" @input="onInput" />
                                </el-form-item>

                                <el-form-item :label="t('login.secretKey')" prop="secretKey">
                                    <el-input v-model="loginForm.secretKey" type="password"
                                        :placeholder="t('login.secretKeyPlaceholder')" size="large" show-password
                                        autocomplete="password" id="secretKey" name="secretKey" @input="onInput" />
                                </el-form-item>

                                <!-- 分割线 -->
                                <div class="section-divider"></div>

                                <!-- 可扩展的存储点和区域设置 -->
                                <div class="expandable-section">
                                    <div class="expandable-header" @click="toggleExpand">
                                        <span class="expandable-title">{{ t('login.expandSettings') }}</span>
                                        <i :class="['fas', expandVisible ? 'fa-chevron-up' : 'fa-chevron-down']"></i>
                                    </div>

                                    <transition name="slide-fade">
                                        <div v-if="expandVisible" class="expandable-content">
                                            <el-form-item :label="t('login.endpoint')" prop="endpoint">
                                                <el-input v-model="loginForm.endpoint"
                                                    :placeholder="t('login.endpointPlaceholder')" size="large" />
                                                <div class="form-item-hint">{{ t('login.endpointHint') }}</div>
                                            </el-form-item>

                                            <el-form-item :label="t('login.region')" prop="region">
                                                <el-input v-model="loginForm.region"
                                                    :placeholder="t('login.regionPlaceholder')" size="large" />
                                                <div class="form-item-hint">{{ t('login.regionHint') }}</div>
                                            </el-form-item>
                                        </div>
                                    </transition>
                                </div>

                                <div class="s3-universal-browser-note">
                                    <i class="fas fa-info-circle"></i>
                                    <span>{{ t('login.s3AKLoginNote') }}</span>
                                </div>
                            </div>

                            <!-- 用户名密码登录表单 -->
                            <div v-if="loginMethod === 'iam'">
                                <el-form-item :label="t('login.username')" prop="username">
                                    <el-input v-model="loginForm.username"
                                        :placeholder="t('login.usernamePlaceholder',{ at: '@' })" size="large"
                                        autocomplete="username" id="username" name="username" @input="onInput" />
                                </el-form-item>

                                <el-form-item :label="t('login.password')" prop="password">
                                    <el-input v-model="loginForm.password" type="password"
                                        :placeholder="t('login.passwordPlaceholder')" size="large" show-password
                                        autocomplete="password" id="password" name="password" @input="onInput" />
                                </el-form-item>

                                <div class="remember-forgot">
                                    <div class="remember-me">
                                        <input type="checkbox" id="remember" v-model="loginForm.remember"
                                            @change="onRememberChange">
                                        <label for="remember">{{ t('login.rememberMe') }}</label>
                                    </div>
                                    <a href="#" class="forgot-password">{{ t('login.forgotPassword') }}</a>
                                </div>
                            </div>

                            <el-form-item>
                                <el-button type="primary" @click="handleLogin" :loading="loading" size="large"
                                    native-type="submit">
                                    {{ loading ? t('login.connecting') :
                                    loginMethod === 's3' ? t('login.connectToS3') : t('login.loginSystem') }}
                                </el-button>
                            </el-form-item>
                        </el-form>
                    </form>

                    <!-- 隐藏表单，专用于触发浏览器密码保存提示 -->
                    <form id="hidden-password-form"
                        style="position: absolute; left: -9999px; opacity: 0; pointer-events: none;">
                        <input type="text" id="hidden-username" name="hidden-username" autocomplete="username">
                        <input type="password" id="hidden-password" name="hidden-password" autocomplete="password">
                        <input type="submit" id="hidden-submit">
                    </form>
                </div>
            </div>
        </div>
    </div>
</template>

<script setup>
import { ref, reactive, watch, computed, onMounted, nextTick } from 'vue';
import { ElMessage } from 'element-plus';
import { useRouter } from 'vue-router';
import { useI18n } from 'vue-i18n';
import { login } from '../api/admin';
import LanguageSwitch from '@/components/LanguageSwitch.vue';

// 国际化
const { t } = useI18n();

// 路由实例
const router = useRouter();

// 表单引用
const loginFormRef = ref(null);

// 状态变量
const loading = ref(false);
const loginMethod = ref('iam'); // 's3' 或 'iam'

// 登录表单数据
const loginForm = reactive({
    accessKeyId: '',
    secretKey: '',
    endpoint: '',
    region: '',
    username: 'boulder',
    password: 'Abcd@1234',
    remember: false
});

// 组件挂载时，检查是否有保存的登录信息
onMounted(() => {
    // 使用setTimeout延迟恢复数据，给浏览器自动填充留出时间
    setTimeout(() => {
        try {
            // 检查用户名和密码字段是否已有值（可能是浏览器自动填充的）
            if (!loginForm.username && !loginForm.accessKeyId) {
                const savedLoginInfo = localStorage.getItem('rememberedLoginInfo');
                if (savedLoginInfo) {
                    const parsedInfo = JSON.parse(savedLoginInfo);
                    // 恢复表单数据
                    if (parsedInfo.username) {
                        loginForm.username = parsedInfo.username;
                        loginMethod.value = 'iam';
                    }
                    if (parsedInfo.accessKeyId) {
                        loginForm.accessKeyId = parsedInfo.accessKeyId;
                        loginMethod.value = 's3';
                        // 恢复endpoint和region
                        if (parsedInfo.endpoint) {
                            loginForm.endpoint = parsedInfo.endpoint;
                        }
                        if (parsedInfo.region) {
                            loginForm.region = parsedInfo.region;
                        }
                    }
                    loginForm.remember = true;
                }
            }
        } catch (error) {
            console.error('Error reading saved login info:', error);
        }
    }, 100);
    
    // 检查浏览器自动填充
    setTimeout(() => {
        checkAutofill();
    }, 500);
});

// 监听浏览器自动填充事件
const checkAutofill = () => {
    // 确保DOM更新完成
    nextTick(() => {
        // 检查用户名字段
        const usernameInput = document.getElementById('username');
        if (usernameInput && usernameInput.value && !loginForm.username) {
            loginForm.username = usernameInput.value;
        }
        
        // 检查密码字段
        const passwordInput = document.getElementById('password');
        if (passwordInput && passwordInput.value && !loginForm.password) {
            loginForm.password = passwordInput.value;
        }
        
        // 检查S3密钥字段
        const secretInput = document.getElementById('secretKey');
        if (secretInput && secretInput.value && !loginForm.secretKey) {
            loginForm.secretKey = secretInput.value;
        }
    });
};

// 监听输入事件
const onInput = () => {
    // 输入时清空自动填充检测的值
    if (loginForm.username) {
        document.getElementById('username').value = loginForm.username;
    }
    if (loginForm.password) {
        document.getElementById('password').value = loginForm.password;
    }
    if (loginForm.secretKey) {
        document.getElementById('secretKey').value = loginForm.secretKey;
    }
};

// 记住我状态变化
const onRememberChange = () => {
    if (!loginForm.remember) {
        localStorage.removeItem('rememberedLoginInfo');
    }
};

// 扩展区域状态
const expandVisible = ref(false);

// 特性数据
const featuresData = computed(() => [
    {
        icon: 'fas fa-plug',
        titleKey: 'login.feature1Title',
        descriptionKey: 'login.feature1Description'
    },
    {
        icon: 'fas fa-piggy-bank',
        titleKey: 'login.feature2Title',
        descriptionKey: 'login.feature2Description'
    },
    {
        icon: 'fas fa-balance-scale',
        titleKey: 'login.feature3Title',
        descriptionKey: 'login.feature3Description'
    },
    {
        icon: 'fas fa-shield-alt',
        titleKey: 'login.feature4Title',
        descriptionKey: 'login.feature4Description'
    }
]);

// 技术图标数据
const techLogos = [
    {
        width: 32,
        height: 32,
        viewBox: '0 0 256 198',
        paths: [
            { d: 'M240.258 197.122H15.741C7.049 197.122 0 190.073 0 181.381V15.741C0 7.049 7.049 0 15.741 0H240.258C248.951 0 256 7.049 256 15.741V181.38C256 190.073 248.951 197.122 240.258 197.122Z', fill: '#232F3E' },
            { d: 'M118.412 145.388C118.412 145.388 92.843 149.915 85.333 124.346C77.824 98.777 105.3 75.854 113.94 70.064C122.58 64.273 128.371 57.764 134.161 58.985C139.951 60.206 140.542 64.273 138.701 67.621C136.86 70.97 119.593 87.526 119.593 87.526C119.593 87.526 146.069 64.273 159.212 63.392C172.355 62.511 179.574 69.73 182.332 76.358C185.09 82.987 185.09 90.206 181.741 94.273C178.393 98.34 172.355 101.108 172.355 101.108L118.412 145.388Z', fill: 'white' },
            { d: 'M116.571 110.471C116.571 110.471 100.015 113.239 95.948 102.497C91.881 91.755 109.738 79.119 116.571 76.358C123.404 73.597 128.371 70.064 131.72 71.285C135.068 72.506 134.477 75.854 133.256 77.695C132.035 79.536 119.593 88.116 119.593 88.116L116.571 110.471Z', fill: '#FF9900' }
        ]
    },
    {
        width: 32,
        height: 32,
        viewBox: '0 0 256 315',
        paths: [
            { d: 'M130.003 313.607L0 236.722V78.98L130.003 1.724L256 78.354V236.722L130.003 313.607Z', fill: '#00C4B3' },
            { d: 'M130.003 313.607L256 236.722V78.98L130.003 155.865V313.607Z', fill: '#00C4B3' },
            { d: 'M130.003 155.865L0 78.98L130.003 1.724V155.865Z', fill: '#00A98F' }
        ]
    },
    {
        width: 32,
        height: 32,
        viewBox: '0 0 256 222',
        paths: [
            { d: 'M127.693 221.704L0 166.557V55.161L127.693 0L255.385 55.161V166.557L127.693 221.704Z', fill: '#0052CC' },
            { d: 'M127.693 221.704L255.385 166.557V55.161L127.693 110.308V221.704Z', fill: '#2684FF' },
            { d: 'M127.693 110.308L0 55.161L127.693 0V110.308Z', fill: '#2684FF' }
        ]
    }
];

// 表单验证规则
const rules = reactive({
    accessKeyId: [
        { required: true, message: t('login.pleaseEnterAccessKeyId'), trigger: 'blur' }
    ],
    secretKey: [
        { required: true, message: t('login.pleaseEnterSecretKey'), trigger: 'blur' }
    ],
    endpoint: [
        {
            required: false,
            message: t('login.pleaseEnterEndpoint'),
            trigger: 'blur'
        }
    ],
    region: [
        {
            required: false,
            message: t('login.pleaseEnterRegion'),
            trigger: 'blur'
        }
    ],
    username: [
        { required: true, message: t('login.pleaseEnterUsername'), trigger: 'blur' }
    ],
    password: [
        { required: true, message: t('login.pleaseEnterPassword'), trigger: 'blur' },
        { min: 6, message: t('login.passwordTooShort'), trigger: 'blur' }
    ]
});

// 切换登录方式
const setLoginMethod = (method) => {
    loginMethod.value = method;
    
    // 切换登录方式后检查自动填充
    setTimeout(() => {
        checkAutofill();
    }, 100);
};

// 切换登录方式时重置表单验证
watch(loginMethod, () => {
    if (loginFormRef.value) {
        loginFormRef.value.clearValidate();
    }
});

// 切换扩展区域显示状态
const toggleExpand = () => {
    expandVisible.value = !expandVisible.value;
    if (loginFormRef.value && !expandVisible.value) {
        loginFormRef.value.clearValidate(['endpoint', 'region']);
    }
};

// 跳转到关于页面
const goToAbout = () => {
    router.push('/about');
};

// 处理登录
const handleLogin = async () => {
    if (!loginFormRef.value) {
        return;
    }

    loginFormRef.value.validate(async (valid) => {
        if (valid) {
            // 复制数据到隐藏表单，触发浏览器保存密码提示
            if (loginMethod.value === 'iam') {
                const hiddenUsername = document.getElementById('hidden-username');
                const hiddenPassword = document.getElementById('hidden-password');
                if (hiddenUsername && hiddenPassword) {
                    hiddenUsername.value = loginForm.username;
                    hiddenPassword.value = loginForm.password;
                }
            }
            
            loading.value = true;
            try {
                let response;
                if (loginMethod.value === 'iam') {
                    response = await login({
                        username: loginForm.username,
                        password: loginForm.password,
                        remember: loginForm.remember
                    });
                } else if (loginMethod.value === 's3') {
                    response = await login({
                        accessKeyId: loginForm.accessKeyId,
                        secretKey: loginForm.secretKey,
                        endpoint: loginForm.endpoint || '',
                        region: loginForm.region || '',
                        remember: loginForm.remember
                    });
                }

                // 检查登录结果
                if (response && response.success) {
                    ElMessage.success(t('login.loginSuccess'));
                    
                    // 保存记住我信息
                    if (loginForm.remember) {
                        try {
                            const loginInfoToSave = {};
                            
                            // 仅保存非敏感信息
                            if (loginMethod.value === 'iam' && loginForm.username) {
                                loginInfoToSave.username = loginForm.username;
                            } else if (loginMethod.value === 's3' && loginForm.accessKeyId) {
                                loginInfoToSave.accessKeyId = loginForm.accessKeyId;
                                // 也可以保存endpoint和region（如果用户输入了）
                                if (loginForm.endpoint) {
                                    loginInfoToSave.endpoint = loginForm.endpoint;
                                }
                                if (loginForm.region) {
                                    loginInfoToSave.region = loginForm.region;
                                }
                            }
                            
                            localStorage.setItem('rememberedLoginInfo', JSON.stringify(loginInfoToSave));
                        } catch (error) {
                            console.error('Error saving login info:', error);
                        }
                    } else {
                        // 如果未勾选记住我，则清除之前保存的信息
                        localStorage.removeItem('rememberedLoginInfo');
                    }
                    
                    // 触发隐藏表单的提交，强制浏览器识别登录事件
                            setTimeout(() => {
                                const hiddenForm = document.getElementById('hidden-password-form');
                                if (hiddenForm) {
                                    hiddenForm.dispatchEvent(new Event('submit', { cancelable: true }));
                                }
                            }, 100);
                            
                            // 添加延迟再导航，给浏览器时间触发密码保存提示
                            setTimeout(() => {
                                router.push('/dashboard');
                            }, 300);
                } else {
                    const message = response?.msg || t('login.loginFailed');
                    ElMessage.error(message);
                }
            } catch (error) {
                // 处理异常情况
                const message = error.response?.data?.msg || t('login.loginFailed');
                ElMessage.error(message);
            } finally {
                loading.value = false;
            }
        } else {
            ElMessage.error(t('login.formValidationFailed'));
            return false;
        }
    });
};
</script>

<!-- 全局变量：放在非 scoped 中 -->
<style>
:root {
    --primary-gradient: linear-gradient(135deg, #6e8efb, #a777e3);
    --primary-light: #f0f5ff;
    --card-bg: rgba(255, 255, 255, 0.85);
    --accent-blue: #4f6df5;
    --accent-purple: #a777e3;
    --accent-cyan: #00c6fb;
    --text-dark: #2d3748;
    --text-muted: #718096;
    --border-light: rgba(203, 213, 225, 0.5);
}

/* 修复Element Plus样式覆盖 */
:root {
    --el-input-border-color: var(--border-light) !important;
    --el-input-hover-border-color: var(--accent-blue) !important;
    --el-input-focus-border-color: var(--accent-blue) !important;
}
</style>

<style scoped>
.login-page {
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    position: relative;
    overflow: hidden;
    background: linear-gradient(135deg, #f0f5ff 0%, #f8fafc 100%);
    font-family: 'Inter', 'PingFang SC', 'Microsoft YaHei', sans-serif;
}

/* 背景效果 */
.background-elements {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    z-index: 0;
    overflow: hidden;
}

.grid-pattern {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-image:
        linear-gradient(rgba(179, 196, 255, 0.1) 1px, transparent 1px),
        linear-gradient(90deg, rgba(179, 196, 255, 0.1) 1px, transparent 1px);
    background-size: 40px 40px;
    opacity: 0.6;
}

.radial-gradient {
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(circle at center, rgba(110, 142, 251, 0.1) 0%, transparent 70%);
    opacity: 0.4;
}

.polygon {
    position: absolute;
    opacity: 0.15;
    filter: blur(30px);
}

.polygon-1 {
    width: 300px;
    height: 300px;
    background: var(--accent-blue);
    top: 15%;
    left: 25%;
    clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
    animation: float 15s ease-in-out infinite;
}

.polygon-2 {
    width: 250px;
    height: 250px;
    background: var(--accent-purple);
    bottom: 15%;
    right: 10%;
    clip-path: polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%);
    animation: float 18s ease-in-out infinite;
    animation-delay: 2s;
}

.polygon-3 {
    width: 200px;
    height: 200px;
    background: var(--accent-cyan);
    top: 40%;
    right: 20%;
    clip-path: polygon(20% 0%, 80% 0%, 100% 20%, 100% 80%, 80% 100%, 20% 100%, 0% 80%, 0% 20%);
    animation: float 12s ease-in-out infinite;
    animation-delay: 4s;
}

.polygon-4 {
    width: 350px;
    height: 350px;
    background: var(--accent-purple);
    bottom: 10%;
    left: 15%;
    clip-path: polygon(0% 15%, 15% 15%, 15% 0%, 85% 0%, 85% 15%, 100% 15%, 100% 85%, 85% 85%, 85% 100%, 15% 100%, 15% 85%, 0% 85%);
    animation: float 20s ease-in-out infinite;
    animation-delay: 1s;
}

@keyframes float {
    0% {
        transform: translate(0, 0) rotate(0deg);
    }

    33% {
        transform: translate(20px, -50px) rotate(60deg);
    }

    66% {
        transform: translate(-30px, 30px) rotate(120deg);
    }

    100% {
        transform: translate(0, 0) rotate(180deg);
    }
}

/* 技术线条 */
.tech-line {
    position: absolute;
    background: linear-gradient(to right, var(--accent-blue), var(--accent-purple));
    opacity: 0.1;
    transform-origin: left;
}

.line-1 {
    width: 80%;
    height: 2px;
    top: 30%;
    left: 0;
    transform: rotate(-20deg);
}

.line-2 {
    width: 70%;
    height: 2px;
    bottom: 20%;
    right: 0;
    transform: rotate(25deg);
}

/* 内容布局 */
.content-wrapper {
    display: flex;
    max-width: 1200px;
    width: 100%;
    gap: 40px;
    z-index: 10;
}

.intro-section {
    flex: 1;
    padding: 40px;
    display: flex;
    flex-direction: column;
    justify-content: center;
}

.login-section {
    flex: 1;
    display: flex;
    justify-content: center;
    align-items: center;
}

/* 介绍样式 */
.logo {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 30px;
}

.logo-icon {
    width: 48px;
    height: 48px;
    background: var(--primary-gradient);
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    box-shadow: 0 4px 10px rgba(110, 142, 251, 0.3);
}

.logo-icon svg {
    width: 28px;
    height: 28px;
    color: white;
}

.logo-text {
    font-size: 24px;
    font-weight: 700;
    background: linear-gradient(to right, #6e8efb, #a777e3);
    background-clip: text;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.tagline {
    font-size: 42px;
    font-weight: 700;
    line-height: 1.2;
    margin-bottom: 20px;
    color: var(--text-dark);
}

.tagline span {
    background: linear-gradient(to right, #6e8efb, #a777e3);
    background-clip: text;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.subtitle {
    font-size: 18px;
    color: var(--text-muted);
    margin-bottom: 40px;
    max-width: 500px;
    line-height: 1.6;
}

.features {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 40px;
}

.feature {
    display: flex;
    gap: 18px;
    background: rgba(255, 255, 255, 0.85);
    padding: 22px;
    border-radius: 16px;
    backdrop-filter: blur(8px);
    border: 1px solid var(--border-light);
    box-shadow: 0 6px 20px rgba(110, 142, 251, 0.08);
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}

.feature:hover {
    transform: translateY(-5px);
    box-shadow: 0 12px 25px rgba(110, 142, 251, 0.15);
    border-color: rgba(110, 142, 251, 0.3);
}

.feature::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 5px;
    height: 100%;
    background: var(--accent-blue);
}

.feature-icon {
    width: 50px;
    height: 50px;
    background: rgba(110, 142, 251, 0.1);
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    font-size: 22px;
    color: var(--accent-blue);
}

.feature-content {
    flex: 1;
}

.feature-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.feature-title {
    font-size: 17px;
    font-weight: 600;
    color: var(--text-dark);
}

.feature-status {
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 20px;
    background: rgba(16, 185, 129, 0.1);
    color: var(--accent-green);
}

.feature-description {
    font-size: 14px;
    color: var(--text-muted);
    line-height: 1.6;
    margin-bottom: 12px;
}

.feature-link {
    display: inline-flex;
    align-items: center;
    color: var(--accent-blue);
    font-size: 13px;
    font-weight: 500;
    text-decoration: none;
    transition: all 0.3s;
}

.feature-link:hover {
    color: var(--accent-purple);
    transform: translateX(3px);
}

.feature-link i {
    margin-left: 5px;
    font-size: 11px;
    transition: all 0.3s;
}

.feature-link:hover i {
    transform: translateX(3px);
}

.tech-logos {
    display: flex;
    gap: 20px;
    align-items: center;
    margin-top: auto;
}

.tech-logo {
    opacity: 0.6;
    transition: opacity 0.3s;
}

.tech-logo:hover {
    opacity: 1;
}

/* 登录卡片 */
.login-card {
    color: #010101;
    background: #f1f7fe;
    backdrop-filter: blur(16px);
    border-radius: 16px;
    width: 100%;
    max-width: 480px;
    padding: 40px;
    border: 1px solid var(--border-light);
    box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.1);
    position: relative;
    overflow: hidden;
    transition: transform 0.3s, box-shadow 0.3s;
}

.login-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 30px 60px -15px rgba(110, 142, 251, 0.3);
}

.login-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 4px;
    background: var(--primary-gradient);
}

.login-header {
    text-align: center;
    margin-bottom: 30px;
    position: relative;
}

.language-switch-container {
    position: absolute;
    top: 0;
    right: 0;
}

.github-link-container {
    position: absolute;
    top: 0;
    right: 160px;
}

.about-link-container {
    position: absolute;
    top: 0;
    right: 80px;
}

.about-link {
    display: flex;
    align-items: center;
    color: var(--text-muted);
    text-decoration: none;
    padding: 8px 12px;
    border-radius: 6px;
    transition: all 0.3s ease;
    font-size: 14px;
}

.about-link:hover {
    color: var(--accent-blue);
    background-color: rgba(79, 109, 245, 0.1);
}

.login-icon {
    width: 80px;
    height: 80px;
    margin: 0 auto 20px;
    background: linear-gradient(135deg, rgba(110, 142, 251, 0.1), rgba(167, 119, 227, 0.1));
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    position: relative;
}

.login-icon::after {
    content: '';
    position: absolute;
    width: 100%;
    height: 100%;
    border-radius: 50%;
    background: linear-gradient(135deg, rgba(79, 109, 245, 0.4), transparent);
    animation: rotate 6s linear infinite;
}

.login-icon svg {
    width: 40px;
    height: 40px;
    color: var(--accent-blue);
    z-index: 2;
}

@keyframes rotate {
    from {
        transform: rotate(0deg);
    }

    to {
        transform: rotate(360deg);
    }
}

.login-title {
    font-size: 28px;
    font-weight: 700;
    margin-bottom: 8px;
    color: var(--text-dark);
}

.login-subtitle {
    color: var(--text-muted);
    font-size: 15px;
}

/* 登录方式切换 */
.login-method-toggle {
    display: flex;
    background: rgba(255, 255, 255, 0.7);
    border-radius: 12px;
    padding: 6px;
    margin-bottom: 24px;
    border: 1px solid var(--border-light);
}

.method-btn {
    flex: 1;
    text-align: center;
    padding: 10px;
    border-radius: 8px;
    cursor: pointer;
    font-weight: 500;
    transition: all 0.3s;
    color: var(--text-muted);
}

.method-btn.active {
    background: var(--primary-gradient);
    color: white;
    box-shadow: 0 4px 10px rgba(110, 142, 251, 0.3);
}

/* 记住我和忘记密码 */
.remember-forgot {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
}

.remember-me {
    display: flex;
    align-items: center;
}

.remember-me input {
    margin-right: 8px;
}

.forgot-password {
    color: var(--accent-blue);
    text-decoration: none;
    font-size: 14px;
    transition: color 0.3s;
}

.forgot-password:hover {
    color: var(--accent-purple);
    text-decoration: underline;
}

/* 表单元素 */
:deep(.el-form-item) {
    margin-bottom: 24px;
}

:deep(.el-form-item__label) {
    color: var(--text-dark);
    font-weight: 500;
    margin-bottom: 8px;
    font-size: 14px;
    display: block;
}

:deep(.el-input__inner) {
    background: rgba(255, 255, 255, 0.7) !important;
    border: 1px solid var(--border-light) !important;
    color: var(--text-dark) !important;
    border-radius: 10px !important;
    padding: 12px 16px !important;
    height: 48px !important;
    transition: all 0.3s !important;
}

:deep(.el-input__inner:focus) {
    border-color: var(--accent-blue) !important;
    box-shadow: 0 0 0 2px rgba(79, 109, 245, 0.2) !important;
}

:deep(.el-input__inner::placeholder) {
    color: #94a3b8 !important;
}

:deep(.el-button) {
    width: 100%;
    height: 50px;
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: 16px !important;
    transition: all 0.3s !important;
    border: none !important;
}

:deep(.el-button--primary) {
    background: var(--primary-gradient) !important;
}

:deep(.el-button--primary:hover) {
    transform: translateY(-2px);
    box-shadow: 0 10px 20px -10px rgba(79, 109, 245, 0.4) !important;
}

:deep(.el-button--primary:active) {
    transform: translateY(0);
}

/* S3通用浏览器说明样式 */
.s3-universal-browser-note {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 12px 16px;
    background: rgba(79, 109, 245, 0.08);
    border: 1px solid rgba(79, 109, 245, 0.2);
    border-radius: 8px;
    margin-top: 16px;
    font-size: 14px;
    color: var(--accent-blue);
}

/* 分割线样式 */
.section-divider {
    height: 1px;
    background: var(--border-light);
    margin: 24px 0;
    width: 100%;
}

/* 扩展区域样式 */
.expandable-section {
    margin: 8px 0;
}

.expandable-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 0;
    cursor: pointer;
    transition: all 0.3s ease;
}

.expandable-header:hover {
    color: var(--accent-blue);
}

.expandable-title {
    font-size: 14px;
    font-weight: 500;
    color: var(--text-dark);
}

.expandable-header i {
    font-size: 12px;
    color: var(--text-muted);
    transition: transform 0.3s ease;
}

.expandable-content {
    margin-top: 12px;
}

/* 确保输入框宽度一致 */
.expandable-content :deep(.el-input__inner) {
    width: 100% !important;
}

.form-item-hint {
    margin-top: 4px;
    font-size: 12px;
    color: var(--text-muted);
    padding-left: 4px;
}

/* 第三方字段动画效果 */
.third-party-fields {
    overflow: hidden;
}

.slide-fade-enter-active {
    transition: all 0.3s ease;
}

.slide-fade-leave-active {
    transition: all 0.2s cubic-bezier(1, 0.5, 0.8, 1);
}

.slide-fade-enter-from,
.slide-fade-leave-to {
    transform: translateY(-10px);
    opacity: 0;
}

.s3-universal-browser-note i {
    font-size: 16px;
    margin-top: 2px;
    flex-shrink: 0;
}

:deep(.el-select .el-input__inner) {
    padding-right: 40px !important;
}

/* 响应式设计 */
@media (max-width: 900px) {
    .intro-section {
        display: none;
    }

    .login-section {
        margin: 0 auto;
        width: 100%;
        max-width: 500px;
    }

    .login-card {
        margin-top: 2rem;
        padding: 30px;
    }
}

@media (max-width: 480px) {
    .login-page {
        padding: 1rem;
    }

    .login-card {
        margin-top: 4rem;
        padding: 25px 20px;
    }

    .login-title {
        font-size: 24px;
    }

    .tagline {
        font-size: 28px;
    }
}
</style>