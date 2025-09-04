"""Service for making LLM API calls."""

import logging
import time
from typing import Dict, List, Any, Optional

import requests

logger = logging.getLogger(__name__)


class LLMService:
    """Service for making LLM API calls to analyze code."""
    
    def __init__(self, endpoint_url: str, token: str):
        """
        Initialize the LLM service.
        
        Args:
            endpoint_url: URL of the LLM endpoint
            token: Authentication token
        """
        self.endpoint_url = endpoint_url
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def call_llm(self, input_text: str, prompt: str, retries: int = 5) -> Dict[str, Any]:
        """
        Make a call to the LLM API.
        
        Args:
            input_text: The input text to analyze
            prompt: The system prompt
            retries: Number of retry attempts
            
        Returns:
            Dict containing the LLM response
        """
        payload = {
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": input_text}
            ]
        }

        wait_time = 2  # seconds
        for attempt in range(retries):
            try:
                response = requests.post(
                    self.endpoint_url, 
                    json=payload, 
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429 and attempt < retries - 1:
                    logger.warning(
                        f"Rate limited (429). Waiting {wait_time} seconds "
                        f"before retrying... (Attempt {attempt + 1})"
                    )
                    time.sleep(wait_time)
                    wait_time *= 2  # exponential backoff
                else:
                    logger.error(f"LLM API call failed: {e}")
                    raise e
            except Exception as e:
                logger.error(f"Unexpected error in LLM call: {e}")
                raise e
        
        # This should never be reached due to the raise statements above
        raise RuntimeError("All retry attempts failed")
    
    def is_descriptive_name(self, name: str) -> tuple[bool, str]:
        """
        Check if a name is descriptive using LLM.
        
        Args:
            name: The name to check
            
        Returns:
            Tuple of (is_descriptive, explanation)
        """
        prompt = (
            "Is the following name descriptive and meaningful for a data platform "
            "activity, command, or notebook? Provide a boolean (True/False) and a "
            "brief explanation."
        )
        
        try:
            result = self.call_llm(name, prompt)
            content = result["choices"][0]["message"]["content"]
            is_descriptive = "True" in content
            return is_descriptive, content
        except Exception as e:
            logger.error(f"Failed to evaluate name descriptiveness: {e}")
            return False, "LLM failed to evaluate name."
    
    def batch_is_descriptive_names(self, titles: List[str], batch_size: int = 5) -> List[tuple[bool, str]]:
        """
        Batch check if titles are descriptive using fewer LLM calls.
        
        Args:
            titles: List of titles to check
            batch_size: Number of titles per batch
            
        Returns:
            List of (is_descriptive, explanation) tuples
        """
        batched_results = []

        for i in range(0, len(titles), batch_size):
            batch = titles[i:i+batch_size]
            
            combined_prompt = (
                "For each title below, tell me if it is descriptive (True/False) "
                "and provide a brief explanation.\n\n"
            )
            for idx, title in enumerate(batch, 1):
                combined_prompt += f"{idx}. {title}\n"
            
            try:
                response = self.call_llm("", combined_prompt)
                answer_text = response["choices"][0]["message"]["content"]
                
                lines = answer_text.strip().splitlines()
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    # Parse: "1. True (meaningful explanation)"
                    parts = line.split(' ', 2)  # ['1.', 'True', '(explanation)']
                    if len(parts) >= 2:
                        is_descriptive = "true" in parts[1].lower()
                        explanation = parts[2] if len(parts) > 2 else ""
                        batched_results.append((is_descriptive, explanation))
                    else:
                        # fallback if parsing fails
                        batched_results.append((False, "LLM parsing error"))
                        
            except Exception as e:
                logger.error(f"Failed batch descriptiveness check: {e}")
                # Add fallback results for this batch
                for _ in batch:
                    batched_results.append((False, f"LLM error: {str(e)}"))

        return batched_results
    
    def has_unnecessary_blank_space(self, code: str) -> tuple[bool, str]:
        """
        Check if code has unnecessary blank lines or spaces.
        
        Args:
            code: The code to analyze
            
        Returns:
            Tuple of (has_unnecessary_space, explanation)
        """
        prompt = (
            "Does the following code contain unnecessary blank lines or excessive "
            "whitespace? Return 'True' if it does, 'False' otherwise, with a brief "
            "explanation."
        )
        
        try:
            result = self.call_llm(code, prompt)
            content = result["choices"][0]["message"]["content"]
            has_unnecessary = "True" in content
            return has_unnecessary, content
        except Exception as e:
            logger.error(f"Failed to check blank space: {e}")
            return False, f"LLM error: {str(e)}"
    
    def is_restartable(self, code: str) -> tuple[bool, str]:
        """
        Check if notebook code is restartable.
        
        Args:
            code: The notebook code to analyze
            
        Returns:
            Tuple of (is_restartable, explanation)
        """
        prompt = (
            "Is the following notebook code restartable (e.g., handles state "
            "appropriately, avoids hard-coded paths)? Return 'True' if restartable, "
            "'False' otherwise, with a brief explanation."
        )
        
        try:
            result = self.call_llm(code, prompt)
            content = result["choices"][0]["message"]["content"]
            is_restartable = "True" in content
            return is_restartable, content
        except Exception as e:
            logger.error(f"Failed to check restartability: {e}")
            return False, f"LLM error: {str(e)}"
    
    def analyze_join_efficiency(self, code: str) -> tuple[bool, str]:
        """
        Analyze SQL joins for efficiency issues.
        
        Args:
            code: The code containing SQL joins to analyze
            
        Returns:
            Tuple of (is_efficient, explanation)
        """
        prompt = (
            "Analyze the following SQL code for join efficiency issues such as "
            "data skew, cartesian products, or missing broadcast hints. "
            "Return 'True' if joins are efficient, 'False' if there are issues, "
            "with a detailed explanation."
        )
        
        try:
            result = self.call_llm(code, prompt)
            content = result["choices"][0]["message"]["content"]
            is_efficient = "True" in content
            return is_efficient, content
        except Exception as e:
            logger.error(f"Failed to analyze join efficiency: {e}")
            return False, f"LLM error: {str(e)}"
    
    def check_sql_injection(self, code: str) -> tuple[bool, str]:
        """
        Check for SQL injection vulnerabilities in dynamic SQL.
        
        Args:
            code: The code to analyze for SQL injection risks
            
        Returns:
            Tuple of (is_secure, explanation)
        """
        prompt = (
            "Analyze the following code for SQL injection vulnerabilities. "
            "Look for dynamic SQL construction using string interpolation, "
            "format strings, or concatenation. Return 'True' if the code is "
            "secure from SQL injection, 'False' if vulnerabilities exist, "
            "with a detailed explanation and suggestions."
        )
        
        try:
            result = self.call_llm(code, prompt)
            content = result["choices"][0]["message"]["content"]
            is_secure = "True" in content
            return is_secure, content
        except Exception as e:
            logger.error(f"Failed to check SQL injection: {e}")
            return False, f"LLM error: {str(e)}"