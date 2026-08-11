app.get('/owner', (req, res) => res.sendFile(path.join(__dirname, 'views', 'owner.html')));
