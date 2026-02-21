/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.perfluencer.common.checks;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Gatling-style fluent XPath extractor for XML message checks.
 * 
 * <p>
 * Uses JDK built-in {@code javax.xml.xpath} — no additional dependencies
 * required.
 * 
 * <h2>Usage:</h2>
 * 
 * <pre>{@code
 * // Extract node text content
 * xpath("/response/status").find().is("OK")
 * 
 * // Check node exists
 * xpath("/response/transactionId").find().exists()
 * 
 * // Count matching nodes
 * xpath("/response/items/item").count().gt(0)
 * 
 * // Transform and validate
 * xpath("/response/amount").find().transform(Double::parseDouble).gt(99.99)
 * }</pre>
 *
 * @author Jakub Dering
 */
public class XPathExtractor {

    private final String expression;
    private String checkName;

    // Thread-local factories to avoid synchronization overhead
    private static final ThreadLocal<DocumentBuilderFactory> DOC_FACTORY = ThreadLocal.withInitial(() -> {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        return factory;
    });

    private static final ThreadLocal<XPathFactory> XPATH_FACTORY = ThreadLocal.withInitial(XPathFactory::newInstance);

    /**
     * Creates a new XPath extractor.
     *
     * @param expression the XPath expression
     */
    public XPathExtractor(String expression) {
        this.expression = expression;
        this.checkName = "xpath(" + expression + ")";
    }

    /**
     * Sets a descriptive name for this check.
     */
    public XPathExtractor named(String name) {
        this.checkName = name;
        return this;
    }

    // ==================== FIND STRATEGIES ====================

    /**
     * Finds the first node matching the XPath and returns its text content.
     *
     * @return CheckStep containing the node text, or null if not found
     */
    public CheckStep<String> find() {
        return new CheckStep<>(checkName, response -> {
            try {
                Document doc = parseXml(response);
                XPath xpath = XPATH_FACTORY.get().newXPath();
                XPathExpression expr = xpath.compile(expression);
                NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
                if (nodes.getLength() > 0) {
                    String text = nodes.item(0).getTextContent();
                    return text != null ? text.trim() : null;
                }
                return null;
            } catch (Exception e) {
                throw new RuntimeException("XPath evaluation failed: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Finds all nodes matching the XPath and returns their text contents.
     *
     * @return CheckStep containing a list of node text values
     */
    public CheckStep<List<String>> findAll() {
        return new CheckStep<>(checkName, response -> {
            try {
                Document doc = parseXml(response);
                XPath xpath = XPATH_FACTORY.get().newXPath();
                XPathExpression expr = xpath.compile(expression);
                NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
                if (nodes.getLength() == 0) {
                    return null;
                }
                List<String> results = new ArrayList<>();
                for (int i = 0; i < nodes.getLength(); i++) {
                    String text = nodes.item(i).getTextContent();
                    results.add(text != null ? text.trim() : "");
                }
                return results;
            } catch (Exception e) {
                throw new RuntimeException("XPath evaluation failed: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Counts the number of nodes matching the XPath expression.
     *
     * @return CheckStep containing the node count
     */
    public CheckStep<Integer> count() {
        return new CheckStep<>(checkName, response -> {
            try {
                Document doc = parseXml(response);
                XPath xpath = XPATH_FACTORY.get().newXPath();
                XPathExpression expr = xpath.compile(expression);
                NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
                return nodes.getLength();
            } catch (Exception e) {
                throw new RuntimeException("XPath evaluation failed: " + e.getMessage(), e);
            }
        });
    }

    // ==================== INTERNAL ====================

    private static Document parseXml(String xml) throws Exception {
        DocumentBuilder builder = DOC_FACTORY.get().newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(xml)));
    }
}
