import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    redirect: '/login'
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import("@/views/Login.vue"),
  },
  {
    path: '/dashboard',
    name: 'MainLayout',
    component: () => import("@/views/Main.vue"),
    children: [
      {
        path: '',
        name: 'Dashboard',
        component: () => import("@/views/Dashboard.vue")
      },
      {
        path: '/buckets',
        name: 'Buckets',
        component: () => import("@/views/Bucket.vue")
      },
      {
        path: '/bucket/:name',
        name: 'BucketBrowser',
        component: () => import("@/views/Browser.vue")
      },
      {
        path: '/accesskey',
        name: 'AccessKey',
        component: () => import("@/views/AccessKey.vue")
      },
      {
        path: '/endpoint',
        name: 'EndPoint',
        component: () => import("@/views/EndPoint.vue")
      },
      {
        path: '/chunk',
        name: 'Chunk',
        component: () => import("@/views/Chunk.vue")
      },
      { path: '/quota',
        name: 'Quota',
        component: () => import("@/views/Quota.vue")
      },
      { path: '/user',
        name: 'User',
        component: () => import("@/views/User.vue")
      },
      { path: '/group',
        name: 'Group',
        component: () => import("@/views/Group.vue")
      },
      { path: '/policy',
        name: 'Policy',
        component: () => import("@/views/Policy.vue")
      },
      { path: '/role',
        name: 'Role',
        component: () => import("@/views/Role.vue")
      },
      { path: '/event',
        name: 'Event',
        component: () => import("@/views/Event.vue")
      },
      {
        path: '/audit',
        name: 'Audit',
        component: () => import("@/views/Audit.vue")
      },
      // 高级功能路由
      {
        path: '/migration',
        name: 'Migration',
        component: () => import("@/views/Migration.vue")
      },
      {
        path: '/defragment',
        name: 'Defragment',
        component: () => import("@/views/Defragment.vue")
      },
      {
        path: '/snapshot',
        name: 'SnapShot',
        component: () => import("@/views/SnapShot.vue")
      },
      {
        path: '/analysis',
        name: 'Analysis',
        component: () => import("@/views/Analysis.vue")
      },
      {
        path: '/debugtool',
        name: 'DebugTool',
        component: () => import("@/views/DebugTool.vue")
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

// 前端路由守卫
router.beforeEach(async (to, from, next) => {
  if (to.path === '/login') {
    next();
    return;
  }

  const res = await fetch('/api/auth/status', { credentials: 'include' });
  if (res.ok) {
    next();
  } else {
    next('/login');
  }
});

export default router