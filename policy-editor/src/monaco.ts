import * as monaco from 'monaco-editor'
import { configureMonacoYaml } from 'monaco-yaml'
import 'monaco-editor/esm/vs/basic-languages/yaml/yaml.contribution'
import YamlWorker from './yaml.worker.js?worker'
import EditorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'
import JsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker'
import solarized from 'monaco-themes/themes/Solarized-light.json'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
monaco.editor.defineTheme('solarized', solarized as any)

const baseUrl = import.meta.env.BASE_URL
const schemaFile = `${baseUrl}policy`

self.MonacoEnvironment = {
    getWorker(_, label) {
        if (label === 'json') {
            return new JsonWorker()
        }
        if (label === 'yaml') {
            return new YamlWorker()
        }
        return new EditorWorker()
    },
}

async function initYamlEditor() {
    const response = await fetch(schemaFile)
    let schema = null
    try {
        schema = await response.json()
    } catch (e) {
        console.error('Failed to parse schema:', e)
        throw e
    }

    configureMonacoYaml(monaco, {
        enableSchemaRequest: true,
        validate: true,
        schemas: [
            {
                fileMatch: ['*'],
                uri: `file://${schemaFile}`,
                schema: schema,
            },
        ],
    })

    const modelUri = monaco.Uri.parse('inmemory://model/example.yaml')
    const template = `# Fill in your model id here, or use "default" if this is going to be the org default
whylabs_dataset_id: <MY-MODEL-ID>
org_id: <MY-ORG-ID>
# These will be controlled by the WhyLabs platform 
id: <ANY-STRING> # This is an arbitrary id that will show up in logs for debugging purposes
schema_version: "0.1.0" # can be 0.1.0, 0.0.1
policy_version: 0

# If you're going to upload this to the WhyLabs platform via our UI or API then the header section
# above is completely ignored, you can just leave it as is. It's only requried when building
# custom policies into the container directly.
`

    // Check if there's content in the URL
    const urlParams = new URLSearchParams(window.location.search)
    const contentFromUrl = urlParams.get('content')
    const theme = urlParams.get('theme') || 'vs-dark'
    const initialContent = contentFromUrl
        ? decodeURIComponent(contentFromUrl)
        : template

    const model = monaco.editor.createModel(initialContent, 'yaml', modelUri)
    const editor = monaco.editor.create(document.getElementById('editor')!, {
        model: model,
        tabSize: 2,
        insertSpaces: true,
        theme,
        minimap: { side: 'left', maxColumn: 80 },
        automaticLayout: true,
    })

    const errorIndicator = document.getElementById('error-indicator')!
    setInterval(() => {
        const markers = monaco.editor.getModelMarkers({ resource: modelUri })
        if (markers.length > 0) {
            errorIndicator.style.display = 'block'
        } else {
            errorIndicator.style.display = 'none'
        }
    }, 750)

    // Add a click event listener to the copy button
    const copyButton = document.getElementById('copy')!
    copyButton.addEventListener('click', () => {
        const editorContent = editor.getValue()
        navigator.clipboard
            .writeText(editorContent)
            .then(() => {
                copyButton.textContent = 'Copied!'
                setTimeout(() => {
                    copyButton.textContent = 'Copy'
                }, 2000)
            })
            .catch(() => {})
    })

    // Update URL when content changes
    editor.onDidChangeModelContent(() => {
        const content = encodeURIComponent(editor.getValue())
        const newUrl = `${window.location.pathname}?content=${content}`
        window.history.replaceState({}, '', newUrl)
    })
}

initYamlEditor()
