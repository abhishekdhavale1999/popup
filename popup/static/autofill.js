document.addEventListener("DOMContentLoaded", () => {
    const emailInput = document.getElementById("email");
    let debounceTimer;
    let lastRequestedEmail = "";
  
    ["input", "paste", "change"].forEach(eventType => {
      emailInput.addEventListener(eventType, handleInput);
    });
  
    function handleInput() {
      const email = emailInput.value.trim();
  
      if (email === "" || !isValidEmail(email)) {
        clearFields();
        return;
      }
  
      clearTimeout(debounceTimer);
  
      debounceTimer = setTimeout(() => {
        lastRequestedEmail = email;
        document.getElementById("source").innerHTML = `<span style="color: gray;">🔄 Fetching company info...</span>`;
  
        fetch("/get-company-info", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email })
        })
          .then(res => res.json())
          .then(data => {
            if (email !== lastRequestedEmail) return;
  
            if (data.error) {
              clearFields();
              document.getElementById("source").textContent = data.error;
              return;
            }
  
            document.getElementById("company_name").value = data.company_name || "";
            document.getElementById("industry").value = data.industry || "";
            document.getElementById("employee_size").value = data.employee_size || "";
            document.getElementById("country").value = data.country || "";
            document.getElementById("source").textContent = `Source: ${data.source}`;
  
            const logoContainer = document.getElementById("logo-container");
            logoContainer.innerHTML = data.logo ? `<img src="${data.logo}" alt="logo" style="height:30px; margin-top:10px;">` : "";
  
            // Show LinkedIn if available
            const linkedinContainer = document.getElementById("linkedin-container");
            linkedinContainer.innerHTML = data.linkedin ? `<a href="${data.linkedin}" target="_blank">🔗 LinkedIn</a>` : "";
          })
          .catch(err => {
            clearFields();
            console.error("Fetch error", err);
            document.getElementById("source").textContent = " Error fetching data.";
          });
      }, 500);
    }
  
    function isValidEmail(email) {
      return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    }
  
    function clearFields() {
      document.getElementById("company_name").value = "";
      document.getElementById("industry").value = "";
      document.getElementById("employee_size").value = "";
      document.getElementById("country").value = "";
      document.getElementById("source").textContent = "";
      document.getElementById("logo-container").innerHTML = "";
      document.getElementById("linkedin-container").innerHTML = "";
    }
  });
  