package de.aspera.dataexport.groovy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.SystemUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import de.aspera.dataexport.util.dataset.editor.DatasetEditorFacade;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import groovy.util.ResourceException;
import groovy.util.ScriptException;

public class GroovyReader {
	public final static String PROJECT_NAME = "dataExporter";
	private File script;
	private GroovyObject groovyObj;
	private static final GroovyClassLoader classLoader = new GroovyClassLoader();

	public GroovyReader() throws GroovyReaderException, CompilationFailedException, IOException {
		readGroovyScript();
	}

	private void readGroovyScript() throws GroovyReaderException {
		script = getGroovyScriptFile();
		try {
			Class groovyClass = classLoader.parseClass(script);
			groovyObj = (GroovyObject) groovyClass.newInstance();
		} catch (InstantiationException | IllegalAccessException | CompilationFailedException | IOException e) {
			throw new GroovyReaderException(e.getMessage());
		}

	}

	private File getGroovyScriptFile() {
		String filePath;
		if (SystemUtils.IS_OS_WINDOWS) {
			filePath = System.getProperty("user.home") + "\\." + PROJECT_NAME + "\\GroovyScript.groovy";
		} else {
			filePath = System.getProperty("user.home") + "/." + PROJECT_NAME + "/GroovyScript.groovy";
		}
		Path pathOfFile = Paths.get(filePath);
		return pathOfFile.toFile();
	}

	public void executeGroovyScript(DatasetEditorFacade facade) throws ResourceException, ScriptException {
		groovyObj.invokeMethod("runScript", facade);
	}

}
