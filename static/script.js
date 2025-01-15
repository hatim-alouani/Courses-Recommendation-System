function getRecommendations() {
    const userId = document.getElementById('user_id').value;
    const loader = document.getElementById('loader');
    const message = document.getElementById('message');
    const recommendationsList = document.getElementById('recommendations');
    const courseTable = document.getElementById('courseTable');
    message.textContent = '';
    recommendationsList.innerHTML = '';
    if (!userId) {
        message.textContent = 'Please enter a user ID!';
        return;
    }
    loader.style.display = 'block';
    fetch(`/recommendations?user_id=${userId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('User not found');
            }
            return response.json();
        })
        .then(data => {
            loader.style.display = 'none';
            if (data.message) {
                message.textContent = data.message;
            } 
            else if (data.length === 0) {
                message.textContent = 'No recommendations found for this user.';
            } else {
                data.forEach(course => {
                    const tr = document.createElement('tr');

                    const tdId = document.createElement('td');
                    tdId.textContent = course.course_id;

                    const tdName = document.createElement('td');
                    tdName.textContent = course.course_name;

                    const tdRating = document.createElement('td');
                    tdRating.textContent = course.rating;

                    tr.appendChild(tdId);
                    tr.appendChild(tdName);
                    tr.appendChild(tdRating);
                    recommendationsList.appendChild(tr);
                });
            }
        })
        .catch(error => {
            loader.style.display = 'none';
            if (error.message === 'User not found') {
                message.textContent = 'User not found in dataset!';
            } else {
                message.textContent = 'Error fetching recommendations. Please try again.';
            }
            console.error('Error:', error);
        });
}
