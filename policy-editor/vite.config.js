import { defineConfig } from 'vite'

export default defineConfig(({ mode }) => {
    console.log('mode:', mode)
    console.log('VERSION:', process.env.VERSION)

    // build target
    const prod = {
        basename: '/',
    }

    // build:standalone target
    // This gets built to s3 and uploaded to the /$(version) path of the bucket
    const standalone = {
        basename: `/${process.env.VERSION}/`,
    }

    // dev target
    const dev = {
        basename: '/',
    }

    let config
    if (mode === 'production') {
        config = prod
    } else if (mode === 'standalone') {
        config = standalone
    } else {
        config = dev
    }

    return {
        base: config.basename,
        build: {
            outDir: mode ? `dist/${mode}` : 'dist/development/',
            emptyOutDir: false,
        },
        server: {
            open: true,
            proxy: {
                '/policy': {
                    target: 'http://localhost:8000/policy',
                    changeOrigin: true,
                    rewrite: (path) => path.replace(/^\/policy/, ''),
                },
            },
        },
    }
})
